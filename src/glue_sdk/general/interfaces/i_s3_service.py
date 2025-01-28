from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

__all__:List[str] = ["IS3Service"] 

class IS3Service(ABC):
    """
    Interface for S3Service to define the required methods.
    """

    @abstractmethod
    def load_json(self, url: str, return_on_failure: Any = None, raise_exception: bool = False) -> Dict:
        """
        Load a JSON file from S3 and return its contents as a dictionary.
        
        :param url: The S3 URL of the JSON file.
        :param return_on_failure: Value to return if the operation fails.
        :param raise_exception: Whether to raise an exception on failure.
        :return: The contents of the JSON file as a dictionary.
        """
        pass

    @abstractmethod
    def load_yaml_from_s3(self, url: str, return_on_failure: Any = None, raise_exception: bool = False) -> Dict:
        """
        Load a YAML file from S3 and return its contents as a dictionary.
        
        :param url: The S3 URL of the YAML file.
        :param return_on_failure: Value to return if the operation fails.
        :param raise_exception: Whether to raise an exception on failure.
        :return: The contents of the YAML file as a dictionary.
        """
        pass

    @abstractmethod
    def list_objects(self, url: str, return_on_failure: Any = None, raise_exception: bool = False) -> List[str]:
        """
        List objects in an S3 bucket with the given prefix.
        
        :param url: The S3 URL to list objects from.
        :param return_on_failure: Value to return if the operation fails.
        :param raise_exception: Whether to raise an exception on failure.
        :return: A list of object keys.
        """
        pass

    @abstractmethod
    def delete_object(self, url: str, return_on_failure: Any = None, raise_exception: bool = False) -> bool:
        """
        Delete a single object from an S3 bucket.
        
        :param url: The S3 URL of the object to delete.
        :param return_on_failure: Value to return if the operation fails.
        :param raise_exception: Whether to raise an exception on failure.
        :return: True if deletion was successful, else False.
        """
        pass

    @abstractmethod
    def delete_s3_objects(self, url: str, return_on_failure: Any = None, raise_exception: bool = False) -> bool:
        """
        Delete multiple objects in an S3 bucket with the given prefix.
        
        :param url: The S3 URL to delete objects from.
        :param return_on_failure: Value to return if the operation fails.
        :param raise_exception: Whether to raise an exception on failure.
        :return: True if deletion was successful, else False.
        """
        pass

    @abstractmethod
    def bucket_exists(self, bucket: str, return_on_failure: Any = None, raise_exception: bool = False) -> bool:
        """
        Check if an S3 bucket exists.
        
        :param bucket: Name of the bucket to check.
        :param return_on_failure: Value to return if the operation fails.
        :param raise_exception: Whether to raise an exception on failure.
        :return: True if bucket exists, else False.
        """
        pass

    @abstractmethod
    def generate_presigned_url(self, bucket: str, key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate a pre-signed URL for an S3 object.
        
        :param bucket: Name of the S3 bucket.
        :param key: S3 object key.
        :param expiration: Time in seconds for the pre-signed URL to remain valid.
        :return: Pre-signed URL as a string, or None if generation failed.
        """
        pass
