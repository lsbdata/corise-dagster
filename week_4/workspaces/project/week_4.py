from typing import List

from dagster import Nothing, String, asset, with_resources
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@asset (config_schema={"s3_key": str},
    required_resource_keys={"s3"}, 
    group_name="corise")
def get_s3_data(context)-> List[Stock]:
    s3_bucket = context.op_config["s3_key"]
    stock_list = [Stock.from_list(stock) for stock in context.resources.s3.get_data(s3_bucket)]
    return stock_list


@asset (
    group_name="corise")
def process_data(stock_list: List[Stock]) -> Aggregation:
    high = Aggregation(date=max(stock_list, key= lambda stock: stock.high).date, high=max(stock_list, key= lambda stock: stock.high).high)
    return high

@asset (required_resource_keys={"redis"}, 
    group_name="corise")
def put_redis_data(context, agg_high):
    context.resources.redis.put_data("highest_stock", 
                                    agg_high.date, 
                                    agg_high.high)



@asset (required_resource_keys={"s3"}, 
    group_name="corise")
def put_s3_data(context, agg_high):
    context.resources.s3.put_data(agg_high.date,agg_high.high)


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources(definitions=[get_s3_data, process_data, put_redis_data, put_s3_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
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
)
