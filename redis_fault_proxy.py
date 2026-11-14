"""Loopback TCP -> private Redis Unix socket fault injector (stdlib only).

Only connections through this instance are affected. Backend/admin connections
bypass it. Faults act on actual bytes/sockets, not Celery/Kombu method results.
"""
import json
import queue
import selectors
import socket
import threading
import time


def resp(data, offset=0):
    """Parse a fixed-length RESP2/RESP3 frame; None means incomplete input.

    Scalar payloads stay bytes; collections/null retain structural types. This
    is a framing inspector, not a replacement for redis-py's response decoder.
    """
    if offset >= len(data):
        return None
    line = data.find(b"\r\n", offset)
    if line < 0:
        return None
    prefix, body, end = data[offset:offset + 1], data[offset + 1:line], line + 2
    if prefix == b"_":
        return None, end
    if prefix in (b"+", b"-", b":", b"#", b",", b"("):
        return body, end
    if prefix in (b"$", b"!", b"="):
        count = int(body)
        if count == -1:
            return None, end
        if len(data) < end + count + 2:
            return None
        return data[end:end + count], end + count + 2
    if prefix in (b"*", b"~", b">", b"%", b"|"):
        count = int(body)
        if count == -1:
            return None, end
        is_map = prefix in (b"%", b"|")
        values = []
        for _ in range(count * (2 if is_map else 1)):
            part = resp(data, end)
            if part is None:
                return None
            value, end = part
            values.append(value)
        if is_map:
            return dict(zip(values[::2], values[1::2])), end
        return values, end
    raise ValueError(f"Unexpected RESP prefix: {prefix!r}")


class FaultProxy:
    def __init__(self, redis_socket, event_path):
        self.redis_socket = redis_socket
        self.events = event_path.open("w")
        self.selector = selectors.DefaultSelector()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.setblocking(False)
        self.port = self.listener.getsockname()[1]
        self.selector.register(self.listener, selectors.EVENT_READ, None)
        self.commands = queue.Queue()
        self.sessions = {}
        self.serial = 0
        self.down = False
        self.armed = None
        self.stopping = False
        self.error = None
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return f"redis://127.0.0.1:{self.port}/0"

    def emit(self, event, **fields):
        self.events.write(json.dumps(dict(event=event, time=time.monotonic(), **fields)) + "\n")
        self.events.flush()

    def action(self, name):
        if self.error:
            raise RuntimeError(self.error)
        done = threading.Event()
        result = {}
        self.commands.put((name, done, result))
        if not done.wait(3):
            raise RuntimeError(f"Proxy did not perform {name}; error={self.error}")
        if "error" in result:
            raise RuntimeError(result["error"])
        return result

    def perform(self, name):
        selected = []
        if name in {"cut", "down"}:
            self.down = name == "down"
            selected = list(self.sessions)
            for session in list(self.sessions.values()):
                self.close_session(session, name)
        elif name == "up":
            self.down = False
            self.armed = None
            for session in list(self.sessions.values()):
                if session["blackhole"]:
                    selected.append(session["id"])
                    self.close_session(session, "rescue-blackhole")
        elif name in {"blackhole_brpop", "blackhole_publish"}:
            role = "BRPOP" if name.endswith("brpop") else "PUBLISH"
            for session in self.sessions.values():
                if role in session["roles"]:
                    selected.append(session["id"])
                    session["blackhole"] = True
            if not selected:
                raise RuntimeError(f"No {role} connection to blackhole")
        elif name in {"ack_before", "ack_after"}:
            self.armed = name
        elif name == "stop":
            self.stopping = True
        else:
            raise ValueError(name)
        self.emit("proxy_action", action=name, sessions=selected)
        return dict(time=time.monotonic(), action=name, sessions=selected)

    def close_session(self, session, reason):
        if session["id"] not in self.sessions:
            return
        self.emit("proxy_closed", connection=session["id"], reason=reason)
        del self.sessions[session["id"]]
        for sock in session["sockets"]:
            try:
                self.selector.unregister(sock)
            except (KeyError, ValueError):
                pass
            sock.close()

    def send(self, session, side, data):
        if not data or session["id"] not in self.sessions:
            return
        session["out"][side] += data
        self.selector.modify(session["sockets"][side], selectors.EVENT_READ | selectors.EVENT_WRITE,
                             (session, side))

    def fault_ack(self, session, event):
        self.emit(event, connection=session["id"], delivery_tag=session.get("ack_tag"))
        self.armed = None
        self.down = True
        for other in list(self.sessions.values()):
            self.close_session(other, event)

    def client_data(self, session, data):
        session["input"] += data
        while session["input"]:
            parsed = resp(session["input"])
            if parsed is None:
                break
            args, end = parsed
            raw = session["input"][:end]
            session["input"] = session["input"][end:]
            command = args[0].decode().upper()
            session["roles"].add(command)
            self.emit("redis_command", connection=session["id"], command=command,
                      dropped=session["blackhole"])
            if self.armed and command == "HDEL" and args[1] == b"unacked":
                session["ack_tag"] = args[2].decode()
                if self.armed == "ack_before":
                    # MULTI/ZREM may already be queued on Redis, but EXEC is
                    # never forwarded: disconnect rolls the transaction back.
                    self.fault_ack(session, "ack_dropped_before_commit")
                    return
                session["drop_ack_reply"] = True
            if not session["blackhole"]:
                self.send(session, 1, raw)

    def server_data(self, session, data):
        if session["blackhole"]:
            self.emit("proxy_dropped_reply", connection=session["id"], bytes=len(data))
            session["response"] += data
            while session["response"]:
                parsed = resp(session["response"])
                if parsed is None:
                    break
                value, end = parsed
                session["response"] = session["response"][end:]
                if isinstance(value, list) and len(value) == 2 and isinstance(value[1], bytes):
                    try:
                        message = json.loads(value[1])
                    except (ValueError, UnicodeDecodeError):
                        continue
                    if isinstance(message, dict) and message.get("headers", {}).get("id"):
                        self.emit("proxy_dropped_delivery", connection=session["id"],
                                  task_id=message["headers"]["id"])
        elif session["drop_ack_reply"]:
            # Redis's EXEC response is an array; receiving it proves the ACK
            # transaction ran. Withhold replies and close so the client cannot
            # know it committed. Simple OK/QUEUED responses are not sufficient.
            session["response"] += data
            while session["response"]:
                parsed = resp(session["response"])
                if parsed is None:
                    break
                value, end = parsed
                session["response"] = session["response"][end:]
                if isinstance(value, list):
                    self.fault_ack(session, "ack_reply_dropped_after_commit")
                    return
        else:
            self.send(session, 0, data)

    def accept(self):
        client, _ = self.listener.accept()
        if self.down:
            client.close()
            self.emit("proxy_refused")
            return
        upstream = socket.socket(socket.AF_UNIX)
        upstream.connect(self.redis_socket)
        client.setblocking(False)
        upstream.setblocking(False)
        self.serial += 1
        session = dict(id=self.serial, sockets=(client, upstream), out=[b"", b""],
                       input=b"", response=b"", blackhole=False, drop_ack_reply=False, roles=set())
        self.sessions[self.serial] = session
        for side, sock in enumerate(session["sockets"]):
            self.selector.register(sock, selectors.EVENT_READ, (session, side))
        self.emit("proxy_opened", connection=self.serial)

    def run(self):
        try:
            while not self.stopping:
                while not self.commands.empty():
                    name, done, result = self.commands.get_nowait()
                    try:
                        result.update(self.perform(name))
                    except Exception as exc:
                        result["error"] = repr(exc)
                    finally:
                        done.set()
                for key, flags in self.selector.select(0.02):
                    if key.data is None:
                        self.accept()
                        continue
                    session, side = key.data
                    if session["id"] not in self.sessions:
                        continue
                    try:
                        if flags & selectors.EVENT_WRITE:
                            count = key.fileobj.send(session["out"][side])
                            session["out"][side] = session["out"][side][count:]
                            if not session["out"][side]:
                                self.selector.modify(key.fileobj, selectors.EVENT_READ, (session, side))
                        if flags & selectors.EVENT_READ:
                            data = key.fileobj.recv(65536)
                            if not data:
                                self.close_session(session, f"EOF-{side}")
                            elif side == 0:
                                self.client_data(session, data)
                            else:
                                self.server_data(session, data)
                    except BlockingIOError:
                        pass
                    except OSError as exc:
                        self.close_session(session, type(exc).__name__)
        except BaseException as exc:
            self.error = repr(exc)
            self.emit("proxy_error", error=self.error)
        finally:
            for session in list(self.sessions.values()):
                self.close_session(session, "proxy-stop")
            self.listener.close()
            self.selector.close()
            self.events.close()

    def close(self):
        if self.thread.is_alive():
            self.action("stop")
            self.thread.join(timeout=4)
        if self.thread.is_alive() or self.error:
            raise RuntimeError(f"Proxy cleanup failed: {self.error}")
