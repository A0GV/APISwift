from ...extensions import s3 
from botocore.exceptions import NoCredentialsError

def getPresignedUrl(key:str):
    return s3.s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket":s3.bucketName, "Key":key},
        ExpiresIn=600
    )
