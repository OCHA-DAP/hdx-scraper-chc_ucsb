import asyncio
import logging
from pathlib import Path
from timeit import default_timer as timer
from typing import List, Optional

from hdx.api.configuration import Configuration

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

        self._user_agent = configuration.get_user_agent()
        self._base_url = configuration["base_url"]
        self._base_file = configuration["base_file"]
        self._start_year = configuration["start_year"]
        self._end_year = configuration["end_year"]

    async def zonal_stats(
        self,
        tif_file: str,
        csv_file: str,
        stat: str="mean",
    ) -> Optional[str]:
        """Asynchronous code to download a tif and calculate zonal stats it.
        Returns a tuple with information

        Args:
            tif_file (str): tif file
            csv_file (str): csv file
            stat (str): Statistic. Defaults to mean

        Returns:
            Optional[str]: csv file if success
        """
        try:
            process = await asyncio.create_subprocess_exec(
                "gdal",
                "raster",
                "zonal-stats",
                tif_file,
                csv_file,
                "--output-format=csv",
                "--overwrite",
                f"--zones={self._boundaries}",
                "--include-field=ISO_3",
                f"--stat={stat}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            async for line in process.stdout:
                logger.info(line.decode().strip())
            async for line in process.stderr:
                logger.error(line.decode().strip())

            await process.wait()
            if process.returncode == 0:
                return csv_file
        except Exception as ex:
            logger.exception(ex)
        return None

    async def run_rsync(
        self, source: str, tif_directory: Path, csv_directory: Path
    ) -> List[str]:
        """Runs rsync asynchronously to get output and error streams

        Args:
            source (str): Source path
            tif_directory (Path): tif directory
            csv_directory (Path): csv directory

        Returns:
            List[str]: List of paths
        """
        process = await asyncio.create_subprocess_exec(
            "rsync",
            "-avv",
            "--include='*.tif",
            "--exclude='*'",
            f"{source}/",
            f"{tif_directory}/",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        paths = []
        zonal_tasks = []
        tif_filename = None

        async def zonal_stats(
            tif_filename: str,
        ) -> None:
            if tif_filename:
                tif_file = tif_directory.joinpath(tif_filename)
                csv_filename = tif_filename.replace(".tif", ".csv")
                csv_file = csv_directory.joinpath(csv_filename)
                zonal_tasks.append(
                    asyncio.create_task(self.zonal_stats(str(tif_file), str(csv_file)))
                )

        async for line in process.stdout:
            line = line.decode().strip()
            logger.info(line)
            if "tif" in line:
                next_tif_filename = line.split()[0]
                await zonal_stats(tif_filename)
                tif_filename = next_tif_filename

        async for line in process.stderr:
            logger.error(line.decode().strip())

        await process.wait()

        await zonal_stats(tif_filename)
        for task in zonal_tasks:
            path = await task
            if path:
                paths.append(path)
        return paths

    def process(self, source: str, tif_directory: Path, csv_directory: Path) -> List[str]:
        """Runs rsync asynchronously to get output and error streams

        Args:
            source (str): Source path
            tif_directory (Path): tif directory
            csv_directory (Path): csv directory

        Returns:
            List{str]: List of paths
        """

        start_time = timer()
        result = asyncio.run(self.run_rsync(source, tif_directory, csv_directory))
        logger.info(f"Execution time: {timer() - start_time} seconds")
        return result
