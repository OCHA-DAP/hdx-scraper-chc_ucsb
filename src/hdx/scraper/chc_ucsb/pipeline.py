#!/usr/bin/python
"""Chc_ucsb scraper"""

import asyncio
import logging
from os.path import join
from typing import List, Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.retriever import Retrieve

from hdx.scraper.chc_ucsb.zonalstats import ZonalStats

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, zonal_stats: ZonalStats, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._zonal_stats = zonal_stats
        self._configuration = configuration
        self._retriever = retriever
        self._downloader = retriever.downloader
        self._tempdir = tempdir

    def generate_dataset(self) -> Optional[Dataset]:
        # To be generated
        dataset_name = None
        dataset_title = None
        dataset_time_period = None
        dataset_tags = None
        dataset_country_iso3 = None

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(dataset_time_period)
        dataset.add_tags(dataset_tags)
        # Only if needed
        dataset.set_subnational(True)
        try:
            dataset.add_country_location(dataset_country_iso3)
        except HDXError:
            logger.error(f"Couldn't find country {dataset_country_iso3}, skipping")
            return

        # Add resources here

        return dataset

    def generate_datasets(self, scenario: str) -> List[Dataset]:
        base_url = self._configuration["base_url"]
        base_file = self._configuration["base_file"]
        start_year = self._configuration["start_year"]
        end_year = self._configuration["end_year"]
        products = self._configuration["products"]

        tasks_info = []
        for product in products:
            for year in range(start_year, end_year + 1):
                for month in range(1, 13):
                    month_str = f"{month:02d}"
                    filename = base_file.format(
                        year=year, month=month_str, product=product
                    )
                    url = f"{base_url}/{scenario}/{month_str}/{filename}.tif"
                    filename = f"{scenario}_{month_str}_{filename}"
                    tasks_info.append((url, filename))
        results = self._zonal_stats.process(tasks_info)
        for result in results:
            rows = []
            for year in range(start_year, end_year + 1):
                for month in range(1, 13):
                    month_str = f"{month:02d}"
                    filename = base_file.format(
                        year=year, month=month_str, product=product
                    )
                    filename = f"{scenario}_{month_str}_{filename}.csv"
                    path = join(self._tempdir, filename)
                    headers, iterator = self._downloader.get_tabular_rows(path)
                    for in_row in iterator:
                        row = {
                            "location_code": in_row["ISO_3"],
                            "year": year,
                            "month": month,
                            "count": in_row["count"],
                        }
                        rows.append(row)
