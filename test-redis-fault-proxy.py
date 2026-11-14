#!/usr/bin/env python3
"""Framing checks for the fault injector; socket/fault behavior is tested live."""
import unittest

from redis_fault_proxy import resp


class RespTests(unittest.TestCase):
    def test_binary_command_and_pipeline_boundary(self):
        first = b"*2\r\n$4\r\nECHO\r\n$4\r\n\x00\r\n\xff\r\n"
        following = b"*1\r\n$4\r\nPING\r\n"
        value, end = resp(first + following)
        self.assertEqual(value, [b"ECHO", b"\x00\r\n\xff"])
        self.assertEqual(end, len(first))
        self.assertEqual(resp(first + following, end), ([b"PING"], len(first + following)))

    def test_every_partial_frame_waits_for_more_bytes(self):
        frames = [b"+OK\r\n", b"$-1\r\n", b"_\r\n", b"%1\r\n+proto\r\n:3\r\n",
                  b"*2\r\n:1\r\n:1\r\n",
                  b"*2\r\n$5\r\nqueue\r\n$4\r\nbody\r\n"]
        for frame in frames:
            for i in range(len(frame)):
                with self.subTest(frame=frame, offset=i):
                    self.assertIsNone(resp(frame[:i]))
            self.assertEqual(resp(frame)[1], len(frame))

    def test_exec_array_is_distinct_from_queued_and_null(self):
        self.assertEqual(resp(b"+QUEUED\r\n")[0], b"QUEUED")
        self.assertEqual(resp(b"*2\r\n:1\r\n:1\r\n")[0], [b"1", b"1"])
        self.assertEqual(resp(b"$-1\r\n"), (None, 5))

    def test_resp3_null_map_and_pubsub_push(self):
        self.assertEqual(resp(b"_\r\n"), (None, 3))
        self.assertEqual(resp(b"%1\r\n+proto\r\n:3\r\n")[0], {b"proto": b"3"})
        self.assertEqual(resp(b">3\r\n+message\r\n+channel\r\n+payload\r\n")[0],
                         [b"message", b"channel", b"payload"])

    def test_unexpected_inspected_frame_rejected(self):
        with self.assertRaises(ValueError):
            resp(b"?invalid\r\n")


if __name__ == "__main__":
    unittest.main()
