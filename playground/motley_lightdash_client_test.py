import os

from storyline.cube.cube_filter import CubeFilterOperator
from storyline.lightdash.lightdash_client import LightdashClient
from storyline.domain.query import (
    SemanticLayerQuery,
    SemanticLayerDimension,
    SemanticLayerMeasure,
    CubeFilter,
)

# Setup Lightdash Client
access_token = os.getenv("LIGHTDASH_ACCESS_TOKEN")
project_uuid = os.getenv("LIGHTDASH_PROJECT_UUID")
instance_url = os.getenv("LIGHTDASH_INSTANCE_URL")


client = LightdashClient(instance_url, access_token, project_uuid)

model_name = "time_series_data"
query = SemanticLayerQuery(
    dimensions=[
        SemanticLayerDimension(name="product", cube_name=model_name),
        SemanticLayerDimension(name="region", cube_name=model_name),
    ],
    measures=[SemanticLayerMeasure(name="total_volume", cube_name=model_name)],
    filters=[
        CubeFilter(
            field_name="region",
            cube_name=model_name,
            operator=CubeFilterOperator.EQUALS,
            values=["REGION 7"],
        )
    ],
)

response = client.load_data(query)
df = response.to_df()
print(df)
print("Success!")
