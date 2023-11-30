import asyncio
import logging
import os
import re
from typing import Any, Callable, List, Union
from urllib.parse import unquote

import aiohttp
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


async def info_request_log(
        url: str,
        session: aiohttp.ClientSession,
        response: aiohttp.client_reqrep.ClientResponse
) -> None:
    msg = (
        f'info_request_log\n'
        f'1. cookies: {session.cookie_jar.filter_cookies(url)}\n'
        f'2. status code: {response.status}\n'
        f'3. {response.headers=}\n'
        f'4. response text: {str(await response.text()).strip() or None}\n'
        f'5. url: {response.url}\n'
        f'\n'
    )
    print(msg)
    for redirect in response.history:
        print(f'- URL: {redirect.url}\n  Status code: {redirect.status}')


async def get_filename_from_response_headers(
        session: aiohttp.ClientSession,
        store_uuid: str
) -> str:
    url = f'https://safe-route.ru/api/docstore/{store_uuid}'
    async with session.get(url) as response:
        if response.status == 200:
            content_disposition = response.headers.get('Content-Disposition')
            if content_disposition and 'filename' in content_disposition:
                filename = re.findall('filename="(.+)"', content_disposition)
                if filename:
                    filename = unquote(filename[0], encoding='utf-8')
                    # Очистка имени файла от запрещенных символов и кавычек
                    filename = re.sub(r'[^\w\d.-]', '_', filename)
                    return re.sub(r'^"|"$', '', filename)


async def download_pdf(
        session: aiohttp.ClientSession,
        file_uuid: str
) -> Union[bool, None]:
    url = f'https://safe-route.ru/api/claim/special_project/download?special_project_uuid={file_uuid}'
    dir_name = 'downloads'
    async with session.get(url) as response:
        if response.status == 200:
            json = await response.json()
            store_uuid = json['store_uuid']
            filename = await get_filename_from_response_headers(session, store_uuid)
            save_path = os.path.join(dir_name, filename) if filename is not None else None
            if save_path is not None:
                os.makedirs(dir_name, exist_ok=True)
                if os.path.exists(save_path):
                    print(f'File {filename} already exists. Skip the download.')
                    return
                with open(save_path, 'wb') as f:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
                return True
        else:
            print(f'Error loading: {response.status}')
            await info_request_log(url, session, response)


async def async_requester(
        session: aiohttp.ClientSession,
        arg_list: List[Any],
        callback_func: Callable[[aiohttp.ClientSession, Any], Any]
) -> tuple[Any]:
    tasks = []
    for arg in arg_list:
        task = asyncio.ensure_future(
            callback_func(session, arg))
        tasks.append(task)
    return await asyncio.gather(*tasks)


async def download_all_files(
        session: aiohttp.ClientSession,
        file_uuids: List[str],
        number: int
) -> None:
    responses = await async_requester(
        session, file_uuids, download_pdf
    )
    successful_downloads = [sd for sd in responses if sd]
    print(
        f'Successful downloads {len(successful_downloads)}/{len(file_uuids)} in account {number=}')


async def get_special_project_file_uuid(
        session: aiohttp.ClientSession,
        claim_uuid: str
) -> Union[str, None]:
    url = f'https://safe-route.ru/api/claim/special_projects?claim_uuid={claim_uuid}'
    async with session.get(url) as response:
        json = await response.json()
        if json:
            try:
                special_project_file_uuid = json[0].get(
                    'special_project_uuid')
            except Exception:
                return
            return special_project_file_uuid


async def get_all_file_uuids(
        session: aiohttp.ClientSession,
        claim_uuids: List[str],
        number: int
) -> List[str]:
    responses = await async_requester(
        session, claim_uuids, get_special_project_file_uuid
    )
    print(f'Successful requests {len(responses)}/{len(claim_uuids)} in account {number=}')
    file_uuids = list(set(responses))
    if file_uuids[-1] is None:
        file_uuids = file_uuids[:-1]
    print(f'Documents are contained in {len(file_uuids)} of them in account {number=}')
    return file_uuids


async def get_all_travel_sheets_uuids(
        session: aiohttp.ClientSession,
        number: int,
        limit: int = 1000
) -> List[str]:
    url = f'https://safe-route.ru/api/claim/claims?{limit=}&sort=-outgoing_date'
    async with session.get(url) as response:
        try:
            json = await response.json()
        except aiohttp.client_exceptions.ContentTypeError:
            await info_request_log(url, session, response)
        travel_sheets = json['data']
        if len(travel_sheets) == limit:
            limit *= 2
            return await get_all_travel_sheets_uuids(session, limit)
        uuids = []
        for travel_sheet in travel_sheets:
            uuid = travel_sheet.get('uuid')
            if uuid is not None:
                uuids.append(uuid)
        print(f'Successfully received all travel sheets in account {number=}')
        return uuids


async def start_parser(headers, number: int):
    async with aiohttp.ClientSession(
            headers=headers,
            cookie_jar=aiohttp.CookieJar()
    ) as session:
        print(f'Start parsing in account {number=}')
        uuids = await get_all_travel_sheets_uuids(session, number)
        file_uuids = await get_all_file_uuids(session, uuids, number)
        await download_all_files(session, file_uuids, number)
        print(f'finish parsing account {number=}')


async def main(kgws_lkp_list: List[str]) -> None:
    for number, kgws_lkp in enumerate(kgws_lkp_list, start=1):
        headers = {
            'cookie': f'kgws_lkp={kgws_lkp}',
        }
        await start_parser(headers, number)


if __name__ == '__main__':
    KGWS_LKP_LIST = os.getenv('KGWS_LKP_LIST', '').split()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(KGWS_LKP_LIST))
