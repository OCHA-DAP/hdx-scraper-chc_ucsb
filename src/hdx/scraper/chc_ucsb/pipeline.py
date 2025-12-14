#!/usr/bin/python
"""Chc_ucsb scraper"""

import calendar
import logging
from os import remove
from pathlib import Path
from shutil import make_archive, rmtree
from typing import Callable, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.retriever import Retrieve

from hdx.scraper.chc_ucsb.tiff_download import TIFFDownload

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        tiff_download: TIFFDownload,
        configuration: Configuration,
        retriever: Retrieve,
        tempdir: str,
    ):
        self._tiff_download = tiff_download
        self._configuration = configuration
        self._retriever = retriever
        self._downloader = retriever.downloader
        self._tempdir = tempdir
        self._base_url = self._configuration["base_url"]
        self._base_file = self._configuration["base_file"]

    def generate_resource(
        self, scenario_path: Path, scenario: str, product: str, month: int
    ) -> Tuple[Resource, str]:
        month_str = f"{month:02d}"
        filename = self._base_file.format(product=product, month=month_str)
        logger.info(f"Generating resource with {filename}.zip")
        tif_directory = scenario_path.joinpath(product, month_str)
        source = f"{self._base_url}/{scenario}/{month_str}"
        tif_directory.mkdir(parents=True, exist_ok=True)
        _ = self._tiff_download.process(source, tif_directory, include=f"*{product}*")
        filename = self._base_file.format(product=product, month=month_str)
        zip_path = str(scenario_path.joinpath(filename))
        make_archive(zip_path, "zip", tif_directory)
        zip_path = f"{zip_path}.zip"
        month_name = calendar.month_name[month]
        resource = Resource(
            {
                "name": filename,
                "description": f"CHC-CMIP6 TMax Extremes per Country for {product} in {month_name}",
            }
        )
        resource.set_format("zipped geotiff")
        resource.set_file_to_upload(zip_path)
        rmtree(tif_directory)
        return resource, zip_path

    def generate_dataset(self, scenario: str) -> Optional[Dataset]:
        start_year = self._configuration["start_year"]
        end_year = self._configuration["end_year"]
        dataset_name = f"chc_ucsb_tmax_{scenario.lower()}"
        dataset_title = f"CHC-CMIP6 TMax Extremes per Country for {scenario}"

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period_year_range(start_year, end_year)
        dataset.add_tags(("climate-weather", "environment"))
        # Only if needed
        dataset.set_subnational(False)
        dataset.add_other_location("world")
        return dataset

    def add_resources(
        self,
        dataset_id: str,
        scenario: str,
        create_resource_in_hdx: Callable[[Resource], None],
    ) -> None:
        scenario_path = Path(self._tempdir, scenario)
        scenario_path.mkdir(exist_ok=True)
        for product in self._configuration["products"]:
            for month in range(1, 13):
                resource, zip_path = self.generate_resource(
                    scenario_path, scenario, product, month
                )
                resource["package_id"] = dataset_id
                create_resource_in_hdx(resource)
                remove(zip_path)
