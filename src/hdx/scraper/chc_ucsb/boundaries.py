from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.retriever import Retrieve


def get_boundaries(configuration: Configuration, retriever: Retrieve) -> str:
    boundaries_info = configuration["boundaries"]
    dataset = Dataset.read_from_hdx(boundaries_info["dataset"])
    resource_name = boundaries_info["resource"]
    for resource in dataset.get_resources():
        if resource["name"] == resource_name:
            _, boundaries = resource.download(retriever=retriever)
            return boundaries
    raise FileNotFoundError("Admin boundaries resource not found!")
