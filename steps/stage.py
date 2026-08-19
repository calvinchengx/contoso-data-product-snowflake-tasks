"""Where the internal stage lives on this host.

WHY THIS IS A MODULE AND NOT FIVE COPIES OF ONE LINE. The stage is the one
piece of shared state between this product and its platform: ingest writes the
vendors' bytes into it, and the warehouse -- a container -- reads them back out
through `COPY INTO`. Both halves have to name the SAME directory, and until the
split they agreed by accident, because the steps and the compose file lived in
one repository and both spelled it `<repo>/stages`.

They no longer do. So the platform passes PRODUCT_STAGE, and mounts exactly
that path into the warehouse. The fallback below is not a guess at what the
platform meant: `<product>/stages` is where the platform points by default, so
a lone clone and a platform-driven run resolve to the same place. If the
platform ever mounts somewhere else, it says so in this variable rather than
leaving ingest writing files that `COPY INTO` cannot see -- a divergence that
would surface as an empty bronze rather than as an error.
"""

from __future__ import annotations

import os
import pathlib

HERE = pathlib.Path(__file__).resolve().parent.parent
STAGE = pathlib.Path(os.environ.get("PRODUCT_STAGE") or HERE / "stages")
