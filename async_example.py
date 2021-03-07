import asyncio
import random
from time import sleep

import aiohttp


def make_messages():
    sleep(1)
    print('Sync generate messages')
    return [{'test': f'test {i}', 'id': i} for i in range(1000)]


def save_results(results):
    print(f'Sync save results {results}')
    sleep(1)


async def send_push_notification(message):
    random_sleep = random.randint(1, 15) / 10
    await asyncio.sleep(random_sleep)
    print(f'Async send push notification {message}')
    return {'saved': True, 'id': message['id']}


async def do_work(concurrency: int, save_batch_size: int):
    current_loop = asyncio.get_event_loop()
    session = aiohttp.ClientSession()
    messages = await current_loop.run_in_executor(None, make_messages)

    coroutines = []
    results = []
    for i, message in enumerate(messages, 1):
        coroutines.append(send_push_notification(message))

        if len(results) == save_batch_size or len(messages) == i:
            coroutines.append(current_loop.run_in_executor(None, save_results, results))
            results = []

        if len(coroutines) == concurrency or len(messages) == i:
            results += await asyncio.gather(*coroutines)
            coroutines = []

    if results:
        current_loop.run_in_executor(None, save_results, results)

    await session.close()


if __name__ == '__main__':
    target_report_batch = 100
    target_concurrency = 100
    loop = asyncio.get_event_loop()
    loop.run_until_complete(do_work(concurrency=target_concurrency, save_batch_size=target_report_batch))
    loop._default_executor.shutdown(wait=True)
    loop.close()
