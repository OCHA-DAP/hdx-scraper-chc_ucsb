#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.chc_ucsb._version import __version__
from hdx.scraper.chc_ucsb.pipeline import Pipeline
from hdx.scraper.chc_ucsb.tiff_download import TIFFDownload

# setup_logging("DEBUG")
logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-chc_ucsb"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: CHC UCSB"


def create_resource_in_hdx(resource: Resource) -> None:
    resource.create_in_hdx()


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access("6e30eb6d-52f9-49de-b2cd-2d68fced05c5")

    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:

        def create_dataset_in_hdx(dataset: Dataset) -> str:
            dataset.create_in_hdx(
                remove_additional_resources=True,
                hxl_update=False,
                updated_by_script=_UPDATED_BY_SCRIPT,
                batch=info["batch"],
            )
            return dataset["id"]

        tempdir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=tempdir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=tempdir,
                save=save,
                use_saved=use_saved,
            )
            tiff_download = TIFFDownload(configuration, tempdir)
            pipeline = Pipeline(tiff_download, configuration, retriever, tempdir)

            for scenario in configuration["scenarios"]:
                dataset = pipeline.generate_dataset(scenario)
                dataset.update_from_yaml(
                    script_dir_plus_file(
                        join("config", "hdx_dataset_static.yaml"), main
                    )
                )
                pipeline.add_resources(
                    dataset, scenario, create_dataset_in_hdx, create_resource_in_hdx
                )


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
