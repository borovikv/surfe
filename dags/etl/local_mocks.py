import datetime
from typing import Any

import awswrangler.s3
import boto3


def list_objects(
        path: str,
        suffix: str | list[str] | None = None,
        ignore_suffix: str | list[str] | None = None,
        last_modified_begin: datetime.datetime | None = None,
        last_modified_end: datetime.datetime | None = None,
        ignore_empty: bool = False,
        chunked: bool = False,
        s3_additional_kwargs: dict[str, Any] | None = None,
):
    """
    Replacement for awswrangler.s3.list_objects so it can work with moto server
    """
    s3_client = boto3.client('s3', endpoint_url='http://moto:3000')
    # On top of user provided ignore_suffix input, add "/"
    ignore_suffix_acc = set("/")
    if isinstance(ignore_suffix, str):
        ignore_suffix_acc.add(ignore_suffix)
    elif isinstance(ignore_suffix, list):
        ignore_suffix_acc.update(ignore_suffix)

    result_iterator = awswrangler.s3._list._list_objects(
        path=path,
        suffix=suffix,
        ignore_suffix=list(ignore_suffix_acc),
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        ignore_empty=ignore_empty,
        s3_client=s3_client,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    if chunked:
        return result_iterator
    return [path for paths in result_iterator for path in paths]
