from ...extensions import s3 
from botocore.exceptions import NoCredentialsError, ClientError

def getPresignedUrl(key: str):
    try:
        # Debug completo
        print(f"=== DEBUG getPresignedUrl ===")
        print(f"Key: {key}")
        print(f"bucketName: {s3.bucketName}")
        print(f"s3_client exists: {s3.s3_client is not None}")
        
        # Verificar credenciales del cliente
        if s3.s3_client:
            print(f"s3_client meta: {s3.s3_client.meta}")
            print(f"Region: {s3.s3_client.meta.region_name}")
        
        if not key:
            raise ValueError("La key no puede estar vacía")
        
        if not s3.bucketName:
            raise ValueError(f"bucketName no está configurado. Valor actual: {s3.bucketName}")
        
        if not s3.s3_client:
            raise ValueError("s3_client no está inicializado")
        
        url = s3.s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": s3.bucketName, "Key": key},
            ExpiresIn=600
        )
        print(f"URL generada exitosamente")
        return url
        
    except (NoCredentialsError, ClientError) as e:
        print(f"Error AWS generando presigned URL: {e}")
        return None
    except Exception as e:
        print(f"Error inesperado en getPresignedUrl")
        print(f"Tipo: {type(e).__name__}")
        print(f"Mensaje: {e}")
        import traceback
        traceback.print_exc()
        return None

def postFile(file, key: str):
    try:
        s3.s3_client.put_object(
            Bucket=s3.bucketName,
            Key=key,
            Body=file.read(),
            ContentType=file.content_type
        )
        return True
    except NoCredentialsError:
        return False
    
def deleteFile(key: str):
    try:
        s3.s3_client.delete_object(
            Bucket=s3.bucketName,
            Key=key
        )
        return True
    except NoCredentialsError:
        return False