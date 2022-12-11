from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op (config_schema={"s3_key": str},
    required_resource_keys={"s3"})
def get_s3_data(context)-> List[Stock]:
    s3_bucket = context.op_config["s3_key"]
    stock_list = [Stock.from_list(stock) for stock in context.resources.s3.get_data(s3_bucket)]
    return stock_list
    
@op (ins={"stock_list": In(dagster_type=List[Stock], description="Stock data")},
    out={"agg_high": Out(dagster_type=Aggregation, description="Highest stock with date")})
def process_data(stock_list: List[Stock]) -> Aggregation:
    high = Aggregation(date=max(stock_list, key= lambda stock: stock.high).date, high=max(stock_list, key= lambda stock: stock.high).high)
    return high


@op (required_resource_keys={"redis"}, 
    ins={"agg_high": In(dagster_type=Aggregation, description="Highest stock with date")})
def put_redis_data(context, agg_high):
    context.resources.redis.put_data("highest_stock", 
                                    agg_high.date, 
                                    agg_high.high)


@op (required_resource_keys={"s3"}, ins={"agg_high": In(dagster_type=Aggregation, description="High agg")})
def put_s3_data(context, agg_high):
    s3_bucket = get_s3_data.config_field.config_type.fields["s3_key"]
    context.resources.s3.put_data(s3_bucket, agg_high)



@graph
def week_3_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(partition_keys=[str(item) for item in range(1, 11)])
def docker_config(partition_key: str):
    return {
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_%s.csv" % partition_key}}},
}

week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config = local,
    resource_defs={"s3": mock_s3_resource, 
                    "redis": ResourceDefinition.mock_resource()}
)


week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config = docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource}, 
    op_retry_policy = RetryPolicy(max_retries=10, delay=1))

week_3_schedule_local = ScheduleDefinition(job= week_3_pipeline_local, cron_schedule="*/15 * * * *")

@schedule(job=week_3_pipeline_docker, cron_schedule="0 * * * *")
def week_3_schedule_docker(context):
    for partition in np.arange(1, 11, 1):
        s3_key = "prefix/stock_%s.csv" % str(partition )
        yield week_3_pipeline_docker.run_request_for_partition(partition_key=s3_key, run_key=s3_key)

@sensor (job=week_3_pipeline_docker, minimum_interval_seconds=30)
def week_3_sensor_docker(context):
    new_files = get_s3_keys(bucket= "dagster", prefix = "prefix", endpoint_url = "http://localstack:4566")
    if new_files:
        for new_file in new_files:
            yield RunRequest(
                run_key=new_file,
                run_config= {
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
                            "ops": {"get_s3_data": {"config": {"s3_key":new_file}}},
                        },)
    else:
        yield SkipReason("No new s3 files found in bucket.")
