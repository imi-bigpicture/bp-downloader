import os
import sys

from .cli import main

try:
    code = main()
    sys.stdout.flush()
except BrokenPipeError:
    # The reader went away (`status | head`). Point stdout at /dev/null so the flush at
    # exit doesn't raise again, and leave quietly.
    os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
    code = 1
raise SystemExit(code)
