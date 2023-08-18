from typing import Optional
import boto3

from botocore.client import BaseClient
from botocore.config import Config


def get_botocore_client(
    service_name: str,
    profile_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    config: Optional[Config] = None,
    endpoint_url: Optional[str] = None,
) -> BaseClient:
    """
    Get an AWS service client.

    Args:
        service_name (str): The name of a service.
        profile_name (Optional[str]): The name of a profile to use. If not given, then the default profile is used.
        aws_access_key_id (Optional[str]): AWS access key ID.
        aws_secret_access_key (Optional[str]): AWS secret access key.
        config (Optional[Config]): Advanced client configuration options.
        endpoint_url (Optional[str]): The complete URL to use for the constructed client.

    Returns:
        - (BaseClient) AWS service client.
    """
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        profile_name=profile_name,
    )
    return session.client(service_name, config=config, endpoint_url=endpoint_url) 