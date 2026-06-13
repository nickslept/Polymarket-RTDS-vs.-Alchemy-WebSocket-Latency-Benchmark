import asyncio
import os
import time
from datetime import datetime

import config
import src.state as state
from src.listeners import validate_api_key
from src.connections import run_connections
from src.hashmap import evict_unmatched_trades
from src.writers import trades_writer, orphans_writer


async def main() -> None:
    # --- Alchemy API key ---
    api_key     = os.environ.get("ALCHEMY_API_KEY")
    alchemy_url = f"wss://polygon-mainnet.g.alchemy.com/v2/{api_key}"
    validate_api_key(api_key, alchemy_url)

    # --- Output paths ---
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    run_ts       = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    trades_path  = os.path.join(config.OUTPUT_DIR, f"trades_{run_ts}.parquet")
    orphans_path = os.path.join(config.OUTPUT_DIR, f"orphans_{run_ts}.parquet")
    print(f"[main] Writing trades  → {trades_path}")
    print(f"[main] Writing orphans → {orphans_path}")

    # --- Initialize shared state ---  
    state.trades_queue  = asyncio.Queue()
    state.orphans_queue = asyncio.Queue()
    state.run_start_ns  = time.perf_counter_ns()

    # --- Launch all coroutines concurrently ---
    await asyncio.gather(
        run_connections(alchemy_url, config.ORDER_FILLED_TOPIC),
        trades_writer(trades_path),
        orphans_writer(orphans_path),
        evict_unmatched_trades(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[main] Successfully shutdown.")