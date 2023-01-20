import boto3
from botocore.config import Config

from aws_timestream_module.core.config import config as _config

session = boto3.Session()
# Recommended Timestream write client SDK configuration:
#  - Set SDK retry count to 10
#  - Use SDK DEFAULT_BACKOFF_STRATEGY
#  - Set RequestTimeout to 20 seconds
#  - Set max connections to 5000 or higher
write_client = session.client(
    'timestream-write',
    region_name=_config.region,
    config=Config(
        region_name=_config.region,
        read_timeout=_config.read_timeout,
        max_pool_connections=_config.max_pool_connections,
        retries=_config.retries
    )
)

query_client = session.client(
    'timestream-query',
    region_name=_config.region,
    config=Config(
        region_name=_config.region
    )
)
