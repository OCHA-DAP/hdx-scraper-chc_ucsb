from pathlib import Path

import pytest
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.chc_ucsb.pipeline import Pipeline


class TestPipeline:
    actual_resources = []

    @pytest.fixture(scope="class")
    def my_tiff_download(self):
        class MyTIFFDownload:
            @staticmethod
            def process(source: str, tif_directory: Path, include: str):
                file_path = tif_directory.joinpath("test.tif")
                file_path.touch()

        return MyTIFFDownload

    @pytest.fixture(scope="class")
    def create_dataset_in_hdx(self):
        def my_create_dataset_in_hdx(dataset: Dataset):
            resource = dataset.get_resource()
            resource["id"] = resource["name"]
            self.actual_resources.append(resource)
            dataset["id"] = dataset["name"]
            return dataset

        return my_create_dataset_in_hdx

    @pytest.fixture(scope="class")
    def create_resource_in_hdx(self):
        def my_create_resource_in_hdx(resource: Resource, dataset: Dataset):
            self.actual_resources.append(resource)
            resource["id"] = resource["name"]
            return resource

        return my_create_resource_in_hdx

    def test_pipeline(
        self,
        configuration,
        fixtures_dir,
        input_dir,
        config_dir,
        my_tiff_download,
        create_dataset_in_hdx,
        create_resource_in_hdx,
    ):
        with temp_dir(
            "TestCHD_UCSB_pipeline",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(my_tiff_download, configuration, retriever, tempdir)
                scenario = configuration["scenarios"][0]
                dataset = pipeline.generate_dataset(scenario)
                assert dataset == {
                    "dataset_date": "[2030-01-01T00:00:00 TO 2030-12-31T23:59:59]",
                    "groups": [{"name": "world"}],
                    "name": "chc_ucsb_tmax_2030_ssp245",
                    "notes": "This climate projection dataset contains global, daily gridded data "
                    "for the SSP245 2030 scenario to be used in the identification and "
                    "monitoring of hydroclimatic extremes.\n"
                    "\n"
                    "The Climate Hazards Center Coupled Model Intercomparison Project "
                    "Phase 6 climate projection dataset (CHC-CMIP6) was developed to "
                    "support the analysis of climate-related hazards, including extreme "
                    "heat conditions, over the recent past and in the near-future. "
                    "Global daily high resolution (0.05°) grids of the Climate Hazards "
                    "InfraRed Temperature with Stations temperature product form the "
                    "basis of the 1983–2016 historical record. Large CMIP6 ensembles "
                    "from the Shared Socioeconomic Pathway 2-4.5 and SSP 5-8.5 scenarios "
                    "were then used to develop high resolution daily 2030 and 2050 "
                    "‘delta’ fields. These deltas were used to perturb the historical "
                    "observations, thereby generating 0.05° 2030 and 2050 temperature "
                    "projections. Finally, monthly counts of frequency of extremes for "
                    "each variable were derived for each time period.\n"
                    "\n"
                    "Two scenarios were used from CMIP6—Shared Socioeconomic Pathway "
                    "(SSP) 2–4.5 and 5–8.537. The SSP245 scenario is based on "
                    "‘middle-of-the-road’ projections of development (SSP2). The SSP585 "
                    "scenario projects rapid fossil fuel development and increased "
                    "global market integration (SSP5). These are generally considered "
                    "the most-likely scenario (SSP245) and the high-emissions scenario "
                    "(SSP585)\n"
                    "\n"
                    "Given the two projection periods 2025–2035 and 2045–2055, "
                    "projections for four CMIP6 scenarios (2030_SSP245, 2030_SSP585, "
                    "2050_SSP245, 2050_SSP585) were derived. Counts of the number of "
                    "extreme days per month were calculated for Tmax for the four "
                    "scenarios. Definitions of extremes for each variable were based on "
                    "two methods: known thresholds (30°C and 40.6°C) and by calculating "
                    "pixel-specific breakpoints using the 95th and 99th percentile.\n"
                    "\n"
                    "30°C and 40.6°C represent moderate and extreme heat exposure. These "
                    "were chosen based on documented thresholds for agricultural and "
                    "human heat stress. For each variable, year, and scenario, the "
                    "number of days surpassing each variables’ thresholds were "
                    "calculated.\n"
                    "\n"
                    "For Tmax for each pixel, the daily 95th and 99th percentiles were "
                    "calculated using 1983–2016 daily data, resulting in a 95th and 99th "
                    "percentile value for each variable. For each of these variables, "
                    "each year (1983–2016), and each of the four scenarios, the number "
                    "of days for each month were calculated at each pixel that surpass "
                    "these percentile-defined extreme values.\n"
                    "\n"
                    "More information can be found in this [article in the Nature "
                    "journal](https://www.nature.com/articles/s41597-024-03074-w) and "
                    "and in this [technical "
                    "documentation](https://data.chc.ucsb.edu/products/CHC_CMIP6/Data_Descriptor_CHC_CMIP6_climate_projection_dataset.pdf).\n",
                    "subnational": "0",
                    "tags": [
                        {
                            "name": "climate-weather",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "environment",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Projected Daily Maximum Temperature Extremes by Country: SSP245 "
                    "2030 Scenario (CHC-CMIP6)",
                }
                pipeline.add_resources(
                    dataset, scenario, create_dataset_in_hdx, create_resource_in_hdx
                )
                # For test purposes dataset and resource ids have been set to names
                # First resource won't have package id as it is added to dataset
                # directly and created when the dataset is created
                assert self.actual_resources == [
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "January",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt30C_01.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_01.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "February",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt30C_02.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_02.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "March",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt30C_03.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_03.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "April",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt30C_04.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_04.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in May",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt30C_05.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_05.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "June",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt30C_06.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_06.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "July",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt30C_07.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_07.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "August",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt30C_08.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_08.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "September",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt30C_09.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_09.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "October",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt30C_10.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_10.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "November",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt30C_11.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_11.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "December",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt30C_12.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_12.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "January",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt40p6C_01.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_01.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "February",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt40p6C_02.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_02.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "March",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt40p6C_03.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_03.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "April",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt40p6C_04.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_04.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "May",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt40p6C_05.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_05.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "June",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt40p6C_06.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_06.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "July",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt40p6C_07.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_07.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "August",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt40p6C_08.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_08.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "September",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt40p6C_09.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_09.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "October",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt40p6C_10.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_10.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "November",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt40p6C_11.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_11.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "December",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt40p6C_12.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_12.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "January",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt95_01.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_01.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "February",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt95_02.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_02.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "March",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt95_03.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_03.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "April",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt95_04.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_04.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in May",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt95_05.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_05.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in June",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt95_06.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_06.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in July",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt95_07.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_07.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "August",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt95_08.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_08.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "September",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt95_09.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_09.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "October",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt95_10.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_10.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "November",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt95_11.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_11.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "December",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt95_12.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_12.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "January",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt99_01.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_01.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "February",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt99_02.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_02.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "March",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt99_03.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_03.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "April",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt99_04.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_04.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in May",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt99_05.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_05.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in June",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt99_06.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_06.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in July",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt99_07.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_07.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "August",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt99_08.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_08.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "September",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt99_09.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_09.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "October",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt99_10.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_10.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "November",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt99_11.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_11.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "December",
                        "format": "geotiff",
                        "id": "Daily_Tmax_cnt_Tmaxgt99_12.zip",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_12.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "January",
                        "format": "geotiff",
                        "id": "Daily_Tmax_monthly_mean_01.zip",
                        "name": "Daily_Tmax_monthly_mean_01.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "February",
                        "format": "geotiff",
                        "id": "Daily_Tmax_monthly_mean_02.zip",
                        "name": "Daily_Tmax_monthly_mean_02.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "March",
                        "format": "geotiff",
                        "id": "Daily_Tmax_monthly_mean_03.zip",
                        "name": "Daily_Tmax_monthly_mean_03.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "April",
                        "format": "geotiff",
                        "id": "Daily_Tmax_monthly_mean_04.zip",
                        "name": "Daily_Tmax_monthly_mean_04.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in May",
                        "format": "geotiff",
                        "id": "Daily_Tmax_monthly_mean_05.zip",
                        "name": "Daily_Tmax_monthly_mean_05.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in June",
                        "format": "geotiff",
                        "id": "Daily_Tmax_monthly_mean_06.zip",
                        "name": "Daily_Tmax_monthly_mean_06.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in July",
                        "format": "geotiff",
                        "id": "Daily_Tmax_monthly_mean_07.zip",
                        "name": "Daily_Tmax_monthly_mean_07.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "August",
                        "format": "geotiff",
                        "id": "Daily_Tmax_monthly_mean_08.zip",
                        "name": "Daily_Tmax_monthly_mean_08.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "September",
                        "format": "geotiff",
                        "id": "Daily_Tmax_monthly_mean_09.zip",
                        "name": "Daily_Tmax_monthly_mean_09.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "October",
                        "format": "geotiff",
                        "id": "Daily_Tmax_monthly_mean_10.zip",
                        "name": "Daily_Tmax_monthly_mean_10.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "November",
                        "format": "geotiff",
                        "id": "Daily_Tmax_monthly_mean_11.zip",
                        "name": "Daily_Tmax_monthly_mean_11.zip",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "December",
                        "format": "geotiff",
                        "id": "Daily_Tmax_monthly_mean_12.zip",
                        "name": "Daily_Tmax_monthly_mean_12.zip",
                    },
                ]
