import asyncio
import random
from loguru import logger


# import aiohttp


async def send_request(url, data):
    """
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=json.loads(data)) as r:
            response = await r.json()
            print(f'Sent:\n{data}.\nGot response:\n{response}')
    """
    await asyncio.sleep(random.random() / 20)  # 0 - 50ms
    # logger.info(f'Sent {len(data)} lines to {url}')
