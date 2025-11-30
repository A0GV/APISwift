from ...extensions import s3 
from botocore.exceptions import NoCredentialsError

def getPresignedUrl(key:str):
    return s3.s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket":s3.bucketName, "Key":key},
        ExpiresIn=600
    )

def postFile(file, key:str):
    try:
        s3.s3_client.put_object(
            Bucket=s3.bucketName,
            Key = key,
            Body = file.read(),
            ContentType = file.content_type
        )
        return True
    except NoCredentialsError:
        return False
