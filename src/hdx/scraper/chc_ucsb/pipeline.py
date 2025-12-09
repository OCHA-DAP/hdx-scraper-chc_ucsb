#!/usr/bin/python
"""Chc_ucsb scraper"""

import logging
from os.path import join
from pathlib import Path
from shutil import rmtree
from typing import List, Optional, Dict

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.utilities.retriever import Retrieve
from hdx.utilities.saver import save_iterable

from hdx.scraper.chc_ucsb.zonalstats import ZonalStats

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        zonal_stats: ZonalStats,
        configuration: Configuration,
        retriever: Retrieve,
        tempdir: str,
    ):
        self._zonal_stats = zonal_stats
        self._configuration = configuration
        self._retriever = retriever
        self._downloader = retriever.downloader
        self._tempdir = tempdir

    def process_scenario_month(self, scenario: str, start_year: int, end_year: int, filename: str) -> Optional[Path]:
        base_url = self._configuration["base_url"]
        base_file = self._configuration["base_file"]
        products = self._configuration["products"]

        scenario_path = Path(self._tempdir, scenario)
        scenario_path.mkdir(exist_ok=True)

        filepaths = []
        for month in range(1, 2):#13):
            month_str = f"{month:02d}"
            filename_m = f"{scenario}_{month_str}.csv"
            csv_directory = scenario_path.joinpath(month_str)
            filepath = Path(csv_directory, filename_m)
            if filepath.exists() and filepath.is_file():
                filepaths.append(filepath)
                continue
            done_file = csv_directory.joinpath("done.txt")
            if done_file.exists():
                csv_files = done_file.read_text().splitlines()
                tif_directory = None
            else:
                source = f"{base_url}/{scenario}/{month_str}"
                tif_directory = csv_directory.joinpath("tifs")
                tif_directory.mkdir(parents=True, exist_ok=True)
                csv_files = self._zonal_stats.process(
                    source, tif_directory, csv_directory
                )
                done_file.write_text("\n".join(csv_files))

            def get_rows():
                for year in range(start_year, end_year + 1):
                    for product in products:
                        filename = base_file.format(
                            year=year, month=month_str, product=product
                        )
                        path = str(csv_directory.joinpath(filename))
                        if path not in csv_files:
                            logger.error(f"File {path} not found!")
                            continue
                        headers, iterator = self._downloader.get_tabular_rows(path, dict_form=True)
                        for in_row in iterator:
                            countryiso3 = in_row["ISO_3"]
                            if not countryiso3:
                                continue
                            mean = in_row["mean"]
                            if mean == "nan" or mean == "-9999":
                                continue
                            row = {
                                "location_code": in_row["ISO_3"],
                                "year": year,
                                "month": month,
                                "product": product,
                                "mean": mean,
                            }
                            yield row
            save_iterable(str(filepath), get_rows())
            filepaths.append(filepath)
#            if tif_directory:
#               rmtree(tif_directory)

        output_file = Path(self._tempdir, filename)
        line_no = 0
        with open(output_file, "w", encoding="utf-8") as outfile:
            for i, file in enumerate(filepaths):
                with open(file, "r", encoding="utf-8") as infile:
                    for j, line in enumerate(infile):
                        if i > 0 and j == 0:
                            continue  # skip header for all but first file
                        outfile.write(line)
                        line_no += 1
        if line_no == 0:
            return None
        return output_file

    def generate_dataset(self, scenario: str) -> Optional[Dataset]:
        start_year = self._configuration["start_year"]
        end_year = self._configuration["end_year"]
        dataset_name = f"chc_ucsb_tmax_{scenario.lower()}"
        filename = f"{dataset_name}_adm0.csv"
        path = self.process_scenario_month(scenario, start_year, end_year, filename)
        if not path:
            logger.error(f"Scenario {scenario} has no rows!")
            return None

        dataset_title = "CHC-CMIP6 TMax Extremes per Country"

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

        # Add resources here
        resource = Resource(
            {
                "name": filename,
                "description": dataset_title,
            }
        )
        resource.set_format("csv")
        resource.set_file_to_upload(str(path))

        dataset.add_update_resource(resource)

        return dataset
