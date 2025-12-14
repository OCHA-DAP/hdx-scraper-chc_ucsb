import asyncio
import logging
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from hdx.utilities.path import temp_dir

from hdx.scraper.chc_ucsb.tiff_download import TIFFDownload


class TestTIFFDownload:
    @pytest.mark.asyncio
    @patch("asyncio.create_subprocess_exec")
    def test_process(
        self,
        mock_create_subprocess_exec,
        caplog,
        configuration,
        fixtures_dir,
        input_dir,
    ):
        """Tests a successful rsync run using mocks for the async subprocess."""
        with temp_dir(
            "TestCHD_UCSB_rsync",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with caplog.at_level(logging.ERROR):
                tiff_download = TIFFDownload(configuration, tempdir)

                source_path = "/src"
                dest_path = Path("/dst")
                include_pattern = "*.tif"

                stdout_lines = [
                    b"receiving incremental file list\n",
                    b"created directory /tmp/hdx-scraper-chc_ucsb/observations/01\n",
                    b"delta-transmission enabled\n",
                    b"./\n",
                    b"Daily_Tmax_1983_01_cnt_Tmaxgt30C.tif\n",
                    b"Daily_Tmax_1983_02_cnt_Tmaxgt30C.tif\n",
                    b"Daily_Tmax_1983_03_cnt_Tmaxgt30C.tif\n",
                ]

                stderr_lines = [b"rsync: some minor warning\n"]

                mock_stdout_stream = AsyncMock()
                mock_stdout_stream.__aiter__.return_value = iter(stdout_lines)

                mock_stderr_stream = AsyncMock()
                mock_stderr_stream.__aiter__.return_value = iter(stderr_lines)

                mock_process = AsyncMock(
                    stdout=mock_stdout_stream,
                    stderr=mock_stderr_stream,
                    wait=AsyncMock(return_value=0),  # Simulate successful exit code
                )

                mock_create_subprocess_exec.return_value = mock_process

                results = tiff_download.process(source_path, dest_path, include_pattern)

                mock_create_subprocess_exec.assert_called_once_with(
                    "rsync",
                    "-avv",
                    f"--include={include_pattern}",
                    "--exclude=*",
                    f"{source_path}/",
                    f"{dest_path}/",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                expected_files = [
                    "Daily_Tmax_1983_01_cnt_Tmaxgt30C.tif",
                    "Daily_Tmax_1983_02_cnt_Tmaxgt30C.tif",
                    "Daily_Tmax_1983_03_cnt_Tmaxgt30C.tif",
                ]
                assert results == expected_files

                # C. Check log calls
                assert "rsync: some minor warning" in caplog.text
