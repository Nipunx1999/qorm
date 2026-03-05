"""Allow running qorm as ``python -m qorm``."""

import sys

from .cli import main

sys.exit(main())
