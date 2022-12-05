from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op (config_schema={"s3_key": String},
    required_resource_keys={"s3"})
def get_s3_data(context)-> List[Stock]:
    s3_bucket = context.op_config["s3_key"]
    stock_list = [Stock.from_list(stock) for stock in context.resources.s3.get_data(s3_bucket)]
    return stock_list
    
@op (ins={"stock_list": In(dagster_type=List[Stock], description="Stock data")},
    out={"agg_high": Out(dagster_type=Aggregation, description="High agg")})
def process_data(context, stock_list: List[Stock]) -> Aggregation:
    high = Aggregation(date=max(stock_list, key= lambda stock: stock.high).date, high=max(stock_list, key= lambda stock: stock.high).high)
    return high


@op (required_resource_keys={"redis"}, ins={"agg_high": In(dagster_type=Aggregation, description="High agg")})
def put_redis_data(context, agg_high):
    context.resources.redis.put_data(str(max(stock_list, key= lambda stock: stock.high).date), high=str(max(stock_list, key= lambda stock: stock.high).high))


@op (required_resource_keys={"s3"}, ins={"agg_high": In(dagster_type=Aggregation, description="High agg")})
def put_s3_data(context, agg_high):
    s3_bucket = get_s3_data.config_field.config_type.fields["s3_key"]
    context.resources.s3.put_data(s3_bucket, agg_high)



@graph
def week_2_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config = local,
    resource_defs={"s3": mock_s3_resource, 
                    "redis": ResourceDefinition.mock_resource()}
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config = docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource}
)
