"""等待控制库就绪；决定表由 db/control/init.sql 初始化。"""

import os
import time

import store

RETRIES = int(os.environ.get("STARTUP_RETRIES", "60"))
INTERVAL = float(os.environ.get("STARTUP_RETRY_INTERVAL", "1"))

for attempt in range(1, RETRIES + 1):
    try:
        with store.CONTROL.connect() as conn:
            conn.execute("SELECT 1")
            conn.execute("SELECT 1 FROM transfers LIMIT 1")
        print(f"control database ready (attempt {attempt})")
        break
    except Exception as exc:  # noqa: BLE001
        print(f"control database not ready (attempt {attempt}): {exc}")
        time.sleep(INTERVAL)
else:
    raise SystemExit("control database never became ready")
