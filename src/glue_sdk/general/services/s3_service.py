from botocore.exceptions import ClientError
from typing import List, Optional, Dict, TYPE_CHECKING, Any
from glue_sdk.models.s3path_model import S3Path
from glue_sdk.core.decorators.decorators import handle_s3_exception
import json
import yaml
from glue_sdk.services.base_service import BaseService
from glue_sdk.core.logger import logger
from glue_sdk.interfaces.i_s3_service import IS3Service

__all__:List[str] = ["S3Service"]

if TYPE_CHECKING:
    from botocore.client import BaseClient

class S3Service(IS3Service,BaseService):
    
    def __init__(self, s3_client: "BaseClient") -> None:
        super().__init__()
        self.s3_client: "BaseClient" = s3_client

    @handle_s3_exception()
    def load_json(self,
                  url: str,
                  return_on_failure: Any = None,
                  raise_exception: bool = False
                  )-> Dict:
        s3path = S3Path(url=url)
        self.log_debug(f"Fetching JSON file from {s3path}")
        response: Dict = self.s3_client.get_object(Bucket=s3path.bucket, Key=s3path.key)
        json_content: str = response['body'].read().decode('utf-8')
        data: Dict = json.loads(json_content)
        return data
    
    @handle_s3_exception()
    def load_yaml_from_s3(self,
                        url: str,
                        return_on_failure: Any = None,
                        raise_exception: bool = False
                        )-> Dict:
        logger.debug(f"Fetching YAML file from {url}")
        s3path = S3Path(url=url)
        
        logger.debug(f"load_yaml_from_s3 buckets is {s3path.bucket} and prefix is {s3path.key} and url:{url}")
        response: Dict = self.s3_client.get_object(Bucket=s3path.bucket, Key=s3path.key)
        logger.debug(f"the yaml is : {response}")
        yaml_content:Dict = yaml.safe_load(response['Body'].read().decode('utf-8'))
        return yaml_content

    @handle_s3_exception()
    def list_objects(self, 
                    url: str,
                    return_on_failure: Any = None,
                    raise_exception: bool = False
                    ) -> List[str]:
        s3path = S3Path(url=url)
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=s3path.bucket, Prefix=s3path.key)
        objects: List = []
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects.append(obj['Key'])
        self.log_debug(f"Objects in s3://{bucket}/{prefix}: {objects}")
        return objects
        
    @handle_s3_exception()
    def delete_object(self,
                    url: str,
                    return_on_failure: Any = None,
                    raise_exception: bool = False
                    ) -> bool:
        """
        Delete a single object from an S3 bucket.

        :param bucket: Name of the S3 bucket.
        :param key: S3 object key to delete.
        :return: True if deletion was successful, else False.
        """
        s3path = S3Path(url=url)
        self.s3_client.delete_object(Bucket=s3path.bucket, Key=s3path.key)
        self.log_info(f"Deleted s3://{s3path.bucket}/{s3path.key}")
        return True
    
    @handle_s3_exception()
    def delete_s3_objects(self, 
                        url: str,
                        return_on_failure: Any = None,
                        raise_exception: bool = False
                        ) -> bool:
        """
        Delete multiple objects in an S3 bucket with the given prefix.

        :param bucket: Name of the S3 bucket.
        :param prefix: Prefix to filter objects to delete.
        :return: True if deletion was successful, else False.
        """
        s3path = S3Path(url=url)
        objects_to_delete: List[str] = self.list_objects(url=s3path.url)
        if not objects_to_delete:
            logger.warning(f"No objects found in s3://{bucket}/{prefix} to delete")
            return True

        # S3 allows up to 1000 objects per delete request
        for i in range(0, len(objects_to_delete), 1000):
            batch: List[str] = objects_to_delete[i:i+1000]
            delete_dict: Dict[str, List[Dict[str, str]]] = {'Objects': [{'Key': key} for key in batch]}
            response: Dict = self.s3_client.delete_objects(Bucket=s3path.bucket, Delete=delete_dict)
            deleted: list = response.get('Deleted', [])
            logger.debug(f"Deleted objects: {[obj['Key'] for obj in deleted]}")
        return True
      
    def bucket_exists(self, 
                    bucket: str,
                    return_on_failure: Any = None,
                    raise_exception: bool = False
                    ) -> bool:
        """
        Check if an S3 bucket exists.

        :param bucket: Name of the bucket to check.
        :return: True if bucket exists, else False.
        """
        self.s3_client.head_bucket(Bucket=bucket)
        print(f"Bucket {bucket} exists.")
        return True
    
    def generate_presigned_url(self, bucket: str, key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate a pre-signed URL for an S3 object.

        :param bucket: Name of the S3 bucket.
        :param key: S3 object key.
        :param expiration: Time in seconds for the pre-signed URL to remain valid.
        :return: Pre-signed URL as a string, or None if generation failed.
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': key},
                ExpiresIn=expiration
            )
            print(f"Generated pre-signed URL for s3://{bucket}/{key}: {url}")
            return url
        except ClientError as e:
            print(f"Failed to generate pre-signed URL for s3://{bucket}/{key}: {e}")
            return None