import asyncio
import logging
from os.path import join
from timeit import default_timer as timer
from typing import Dict, List, Tuple

import aiohttp
from aiohttp import ClientResponseError, ConnectionTimeoutError
from aiolimiter import AsyncLimiter
from hdx.api.configuration import Configuration
from tqdm.asyncio import tqdm_asyncio

logger = logging.getLogger(__name__)


class ZonalStats:
    """ZonalStats class"""

    def __init__(
        self,
        configuration: Configuration,
        boundaries: str,
        tempdir: str,
    ) -> None:
        self._boundaries = boundaries
        self._tempdir = tempdir

        # Limit connections per second to a host
        self._rate_limiter = AsyncLimiter(2, 1)

        self._user_agent = configuration.get_user_agent()
        self._base_url = configuration["base_url"]
        self._base_file = configuration["base_file"]
        self._start_year = configuration["start_year"]
        self._end_year = configuration["end_year"]


    async def fetch(
        self,
        url: str,
        filename: str,
        session: aiohttp.ClientSession,
    ) -> Tuple:
        """Asynchronous code to download a resource and hash it with rate
        limiting and exception handling. Returns a tuple with resource
        information including hashes.

        Args:
            url (str): Url of file
            filename (str): Filename
            session (Union[aiohttp.ClientSession, RateLimiter]): session to use for requests

        Returns:
            Tuple: Information
        """
        async with session.get(url, allow_redirects=True, chunked=True) as response:
            status = response.status
            if status != 200:
                exception = ClientResponseError(
                    code=status,
                    message=response.reason,
                    request_info=response.request_info,
                    history=response.history,
                )
                raise exception

            input_path = join(self._tempdir, f"{filename}.tif")
            output_path = join(self._tempdir, f"{filename}.csv")

            with open(input_path, "wb") as f:
                async for chunk in response.content.iter_any():
                    f.write(chunk)

            process = await asyncio.create_subprocess_exec(
                "gdal",
                "raster",
                "zonal-stats",
                input_path,
                output_path,
                "--output-format=csv",
                "--overwrite",
                f"--zones={self._boundaries}",
                "--include-field=ISO_3",
                "--stat=count",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            if process.returncode != 0:
                logger.error(f"Command failed: {stderr.decode()}")
            return url, status, 0

    async def process_task(
        self,
        task_info: Tuple,
        session: aiohttp.ClientSession,
    ) -> Tuple:
        """Asynchronous code to download a tif and calculate zonal stats it.
        Returns a tuple with information

        Args:
            task_info (Tuple): Resource to be checked
            session (Union[aiohttp.ClientSession, RateLimiter]): session to use for requests

        Returns:
            Tuple: Resource information including hash
        """
        url = task_info[0]
        filename = task_info[1]

        async with self._rate_limiter:
            try:
                return await self.fetch(url, filename, session)
            except ConnectionTimeoutError as ex:
                logger.debug(ex)
                return url, filename, 408
            except ClientResponseError as ex:
                logger.error(f"{ex.status} {ex.message} {ex.request_info.url}")
                return url, filename, ex.status
            except Exception as ex:
                logger.exception(ex)
                return url, filename, -99

    async def process_tasks(self, tasks_info: List[Tuple]) -> Dict[str, Tuple]:
        """Asynchronous code to download urls and process them. Return dictionary with
        resources information including hashes.

        Args:
            tasks_info (List[Tuple]): List of task infos

        Returns:
            Dict[str, Tuple]: Process information
        """
        tasks = []

        # Maximum simultaneous connections to a host
        conn = aiohttp.TCPConnector(limit_per_host=2)
        # Can set some timeouts here if needed
        timeout = aiohttp.ClientTimeout(connect=20, total=300)
        async with aiohttp.ClientSession(
            connector=conn,
            timeout=timeout,
            headers={"User-Agent": self._user_agent},
        ) as session:
            for task_info in tasks_info:
                task = self.process_task(task_info, session)
                tasks.append(task)
            responses = {}
            for f in tqdm_asyncio.as_completed(tasks, total=len(tasks)):
                (
                    url,
                    filename,
                    status,
                ) = await f

                responses[url] = (
                    filename,
                    status,
                )
            return responses

    def process(self, tasks_info: List[Tuple]) -> Dict[str, Tuple]:
        """Download urls and process them. Return dictionary with process information

        Args:
            tasks_info (List[Tuple]): List of task infos

        Returns:
            Dict[str, Tuple]: Process information
        """

        start_time = timer()
        results = asyncio.run(self.process_tasks(tasks_info))
        logger.info(f"Execution time: {timer() - start_time} seconds")
        return results
