from pathlib import Path

import pytest
from hdx.data.resource import Resource
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.chc_ucsb.pipeline import Pipeline
from hdx.scraper.chc_ucsb.tiff_download import TIFFDownload


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
    def create_resource_in_hdx(self):
        def my_create_resource_in_hdx(resource: Resource):
            self.actual_resources.append(resource)

        return my_create_resource_in_hdx

    def test_pipeline(
        self,
        configuration,
        fixtures_dir,
        input_dir,
        config_dir,
        my_tiff_download,
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
                tiff_download = TIFFDownload(configuration, tempdir)
                assert tiff_download._base_file == configuration["base_file"]
                pipeline = Pipeline(my_tiff_download, configuration, retriever, tempdir)
                scenario = configuration["scenarios"][0]
                dataset = pipeline.generate_dataset(scenario)
                assert dataset == {
                    "dataset_date": "[1983-01-01T00:00:00 TO 2016-12-31T23:59:59]",
                    "groups": [{"name": "world"}],
                    "name": "chc_ucsb_tmax_2030_ssp245",
                    "subnational": "0",
                    "title": "CHC-CMIP6 TMax Extremes per Country for 2030_SSP245",
                }
                pipeline.add_resources("1234", scenario, create_resource_in_hdx)
                assert self.actual_resources == [
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "January",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_01",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "February",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_02",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "March",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_03",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "April",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_04",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in May",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_05",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "June",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_06",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "July",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_07",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "August",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_08",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "September",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_09",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "October",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_10",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "November",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_11",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt30C in "
                        "December",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt30C_12",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "January",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_01",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "February",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_02",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "March",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_03",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "April",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_04",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "May",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_05",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "June",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_06",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "July",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_07",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "August",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_08",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "September",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_09",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "October",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_10",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "November",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_11",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt40p6C in "
                        "December",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt40p6C_12",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "January",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_01",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "February",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_02",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "March",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_03",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "April",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_04",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in May",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_05",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in June",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_06",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in July",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_07",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "August",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_08",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "September",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_09",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "October",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_10",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "November",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_11",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt95 in "
                        "December",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt95_12",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "January",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_01",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "February",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_02",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "March",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_03",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "April",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_04",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in May",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_05",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in June",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_06",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in July",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_07",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "August",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_08",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "September",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_09",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "October",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_10",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "November",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_11",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for cnt_Tmaxgt99 in "
                        "December",
                        "format": "geotiff",
                        "name": "Daily_Tmax_cnt_Tmaxgt99_12",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "January",
                        "format": "geotiff",
                        "name": "Daily_Tmax_monthly_mean_01",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "February",
                        "format": "geotiff",
                        "name": "Daily_Tmax_monthly_mean_02",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "March",
                        "format": "geotiff",
                        "name": "Daily_Tmax_monthly_mean_03",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "April",
                        "format": "geotiff",
                        "name": "Daily_Tmax_monthly_mean_04",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in May",
                        "format": "geotiff",
                        "name": "Daily_Tmax_monthly_mean_05",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in June",
                        "format": "geotiff",
                        "name": "Daily_Tmax_monthly_mean_06",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in July",
                        "format": "geotiff",
                        "name": "Daily_Tmax_monthly_mean_07",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "August",
                        "format": "geotiff",
                        "name": "Daily_Tmax_monthly_mean_08",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "September",
                        "format": "geotiff",
                        "name": "Daily_Tmax_monthly_mean_09",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "October",
                        "format": "geotiff",
                        "name": "Daily_Tmax_monthly_mean_10",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "November",
                        "format": "geotiff",
                        "name": "Daily_Tmax_monthly_mean_11",
                        "package_id": "1234",
                    },
                    {
                        "description": "CHC-CMIP6 TMax Extremes per Country for monthly_mean in "
                        "December",
                        "format": "geotiff",
                        "name": "Daily_Tmax_monthly_mean_12",
                        "package_id": "1234",
                    },
                ]
