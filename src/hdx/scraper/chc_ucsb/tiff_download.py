import asyncio
import logging
from pathlib import Path
from timeit import default_timer as timer
from typing import List

logger = logging.getLogger(__name__)


class TIFFDownload:
    """TIFFDownload class"""

    async def run_rsync(
        self, source: str, tif_directory: Path, include: str
    ) -> List[str]:
        """Runs rsync asynchronously to get output and error streams

        Args:
            source (str): Source path
            tif_directory (Path): tif directory
            include (str): Files to include

        Returns:
            List[str]: List of paths
        """
        process = await asyncio.create_subprocess_exec(
            "rsync",
            "-avv",
            f"--include={include}",
            "--exclude=*",
            f"{source}/",
            f"{tif_directory}/",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        paths = []
        async for line in process.stdout:
            line = line.decode().strip()
            logger.info(line)
            if "tif" in line:
                tif_filename = line.split()[0]
                paths.append(tif_filename)

        async for line in process.stderr:
            logger.error(line.decode().strip())

        await process.wait()

        return paths

    def process(self, source: str, tif_directory: Path, include: str) -> List[str]:
        """Runs rsync asynchronously to get output and error streams

        Args:
            source (str): Source path
            tif_directory (Path): tif directory
            include (str): Files to include

        Returns:
            List{str]: List of paths
        """

        start_time = timer()
        result = asyncio.run(self.run_rsync(source, tif_directory, include))
        logger.info(f"Execution time: {timer() - start_time} seconds")
        return result
