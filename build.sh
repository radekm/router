#!/usr/bin/env bash
set -e

c3c compile router/main.c3 router/os.c3 router/types.c3 router/ring_buf.c3 router/process_msg.c3  -o main
