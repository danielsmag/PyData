import re
from pydantic import BaseModel, Field,ConfigDict
from typing import Optional, Any

_s3_pattern: re.Pattern = re.compile(
        pattern=r'^s3a?://'
        r'(?=[a-z0-9])'  # Bucket name must start with a letter or digit
        r'(?!(^xn--|sthree-|sthree-configurator|.+-s3alias$))'  # Bucket name must not start with xn--, sthree-, sthree-configurator or end with -s3alias
        r'(?!.*\.\.)'  # Bucket name must not contain two adjacent periods
        r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]'  # Bucket naming constraints
        r'(?<!\.-$)'  # Bucket name must not end with a period followed by a hyphen
        r'(?<!\.$)'  # Bucket name must not end with a period
        r'(?<!-$)'  # Bucket name must not end with a hyphen
        r'(/([a-zA-Z0-9._-]+/?)*)?$'  # key naming constraints
    ) 

_bucket_and_key_pattern: re.Pattern[str] = re.compile(
    pattern=r'^s3a?://'
    r'(?P<bucket>' 
        r'(?=[a-z0-9])'
        r'(?!'
            r'(xn--|sthree-|sthree-configurator|.+-s3alias$)'
        r')'
        r'(?!.*\.\.)'
        r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]'
        r'(?<!\.-$)'
        r'(?<!\.$)'
        r'(?<!-$)'
    r')' # End of bucket named group
    r'(?:/(?P<key>.*))?$'  # Start of key named group (optional)
)

class S3Path(BaseModel):
    model_config = ConfigDict(frozen=True)
    
    url: str = Field(
        ...,
        pattern=_s3_pattern,
        min_length=8,
        max_length=1023,
        description="An S3 path must start with s3:// or s3a:// and conform to S3 URL specifications."
    )
    
    @property
    def fast_url(self) -> str:
        """Get the S3 URL but using the s3a:// protocol."""
        if self.scheme == "s3a":
            return self.url
        else:
            return f"s3a://{self.bucket}/{self.key}"
    
    @property
    def scheme(self) -> str:
        """Extract the scheme from the URL."""
        return self.url.split("://", 1)[0]

    @property
    def bucket(self) -> str:
        """Extract the bucket name from the URL."""
        match: re.Match[str] | None = _bucket_and_key_pattern.match(self.url)
        if not match:
            raise
        return match.group('bucket')
    
    @property
    def is_dir(self) -> bool:
        """Check if the URL is a directory."""
        return self.url.endswith('/')

    @property
    def key(self) -> str:
        """Extract the key from the URL, if present."""
        match: re.Match[str] | None = _bucket_and_key_pattern.match(self.url)
        if not match:
            raise
        key: str | Any = match.group('key')
        return key if key else ""
    
    @property
    def parent(self) -> Optional["S3Path"]:
        """Extract the parent key from the URL, if present."""
        match: re.Match[str] | None = _bucket_and_key_pattern.match(self.url)
        if not match:
            raise
        key = match.group('key')
        if key:
            key_parts = key.removesuffix('/').rsplit('/', 1)
            if len(key_parts) == 1:
                return S3Path(url=f"{self.scheme}://{self.bucket}/")
            else:
                return S3Path(url=f"{self.scheme}://{self.bucket}/{key_parts[0]}/")
        return None
    
