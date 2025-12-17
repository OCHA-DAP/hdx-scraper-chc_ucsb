#!/usr/bin/python
"""Chc_ucsb scraper"""

import calendar
import logging
import os
import subprocess
from os import remove
from pathlib import Path
from shutil import rmtree
from typing import Callable, List, Optional, Tuple

from deterministic_zip_go import exec
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
        self._zip_file = self._configuration["zip_file"]

    def make_deterministic_zip(self, zip_path, tif_directory):
        # Ensure absolute path for the output zip
        abs_zip_path = os.path.abspath(zip_path)

        # 2. Run deterministic-zip
        # We use cwd=tif_directory and zip "." to match shutil's behavior
        # of zipping the contents at the root of the archive.
        process = exec.create_subprocess(
            ["-r", abs_zip_path, "."],
            cwd=tif_directory,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stdout, stderr = process.communicate()

        if process.returncode != 0:
            print(f"Error creating zip: {stderr.decode()}")

    def generate_resource(
        self, scenario_path: Path, scenario: str, product: str, month: int
    ) -> Tuple[Resource, str]:
        month_str = f"{month:02d}"
        filename = self._zip_file.format(product=product, month=month_str)
        logger.info(f"Generating resource with {filename}")
        tif_directory = scenario_path.joinpath(product, month_str)
        source = f"{self._base_url}/{scenario}/{month_str}"
        tif_directory.mkdir(parents=True, exist_ok=True)
        _ = self._tiff_download.process(source, tif_directory, include=f"*{product}*")
        zip_path = str(scenario_path.joinpath(filename))
        self.make_deterministic_zip(zip_path, tif_directory)
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
        year = scenario[:4]
        dataset_name = f"chc_ucsb_tmax_{scenario.lower()}"
        scenario_print = f"{scenario[5:]} {year}"
        dataset_title = f"Projected Daily Maximum Temperature Extremes by Country: {scenario_print} Scenario (CHC-CMIP6)"
        dataset_description = self._configuration["dataset_description"].format(
            scenario=scenario_print
        )

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
                "notes": dataset_description,
            }
        )

        dataset.set_time_period_year_range(year)
        dataset.add_tags(("climate-weather", "environment"))
        # Only if needed
        dataset.set_subnational(False)
        dataset.add_other_location("world")
        return dataset

    def add_resources(
        self,
        dataset: Dataset,
        scenario: str,
        create_dataset_in_hdx: Callable[[Dataset], Dataset],
        create_resource_in_hdx: Callable[[Resource, Dataset], Resource],
    ) -> List[str]:
        scenario_path = Path(self._tempdir, scenario)
        scenario_path.mkdir(exist_ok=True)
        product = self._configuration["products"][0]
        resource, zip_path = self.generate_resource(scenario_path, scenario, product, 1)
        resource = dataset.add_update_resource(resource)
        resource_id = resource.get("id")
        dataset = create_dataset_in_hdx(dataset)
        if not resource_id:
            for res in dataset.get_resources():
                if resource["name"] == res["name"]:
                    resource_id = res["id"]
                    break
        if not resource_id:
            raise ValueError("No resource id for first resource!")
        resource_ids = [resource_id]

        remove(zip_path)

        def add_resource(product: str, month: int) -> None:
            resource, zip_path = self.generate_resource(
                scenario_path, scenario, product, month
            )
            resource = create_resource_in_hdx(resource, dataset)
            resource_ids.append(resource["id"])
            remove(zip_path)

        for month in range(2, 13):
            add_resource(product, month)

        for product in self._configuration["products"][1:]:
            for month in range(1, 13):
                add_resource(product, month)

        return resource_ids
