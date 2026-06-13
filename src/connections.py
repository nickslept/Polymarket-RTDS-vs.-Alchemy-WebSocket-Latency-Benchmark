import asyncio

import config
import src.state as state
from src.hashmap import clear_hashmap_on_disconnect
from src.listeners import polymarket_listener, alchemy_listener


async def run_connections(alchemy_url: str, order_filled_topic: str) -> None:
    backoff = config.RECONNECT_BASE_SECONDS

    while True:
        state.data_valid = False
        poly_ready    = asyncio.Event()
        alchemy_ready = asyncio.Event()

        poly_task    = asyncio.create_task(polymarket_listener(poly_ready), name="Polymarket")
        alchemy_task = asyncio.create_task(
            alchemy_listener(alchemy_ready, alchemy_url, order_filled_topic), name="Alchemy"
        )

        try:
            await asyncio.wait_for(
                asyncio.gather(poly_ready.wait(), alchemy_ready.wait()),
                timeout=config.SUB_ACK_TIMEOUT_SECONDS,
            ) # wait for both listeners to be ready before proceeding
            state.data_valid = True
            backoff          = config.RECONNECT_BASE_SECONDS
            print("[connections] Both subscriptions live. Collecting data...")

            done, _ = await asyncio.wait(
                [poly_task, alchemy_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                try:
                    exc = task.exception()
                    if exc:
                        print(f"[connections] {task.get_name()} dropped with error: {exc}")
                    else:
                        print(f"[connections] {task.get_name()} connection closed cleanly.")
                except asyncio.CancelledError:
                    pass

        except asyncio.TimeoutError:
            print(
                f"[connections] Subscription ack timed out after "
                f"{config.SUB_ACK_TIMEOUT_SECONDS} seconds. Will try to reconnect."
            )
        except Exception as e:
            print(f"[connections] Unexpected error: {e}")

        finally:
            state.data_valid = False
            for task in [poly_task, alchemy_task]:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except (asyncio.CancelledError, Exception):
                        pass
                    print(f"[connections] {task.get_name()} subscription cancelled.")
            clear_hashmap_on_disconnect()
            print(f"[connections] Hashmap cleared.")

        print(f"[connections] Reconnecting in {backoff} seconds...")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, config.RECONNECT_MAX_SECONDS)