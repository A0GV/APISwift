import pymssql 
import boto3

cnx = None

class MSSqlClient:
    def __init__(self):
        self.creds = None

    def init_app(self, config):
        self.creds = config
        return self.reconnect()

    def get_mssql_connection(self):
        global cnx
        if cnx is None: 
            return self.reconnect()
        try:
            cursor = cnx.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
        except Exception:
            return self.reconnect()
        
        return cnx
    
    def reconnect(self):
        global cnx
        
        if self.creds is None:
            raise RuntimeError("No se inicializó la conexión con la base de datos. Corre init_app de MSSQLconnect")
        
        if cnx is not None:
            try:
                cnx.close()
            except Exception:
                pass

        cnx = pymssql.connect(
            server=self.creds['DB_HOST'],
            user=self.creds['DB_USER'],
            password=self.creds['DB_PASSWORD'],
            database=self.creds['DB_NAME'])
        
        return cnx
    

db = MSSqlClient()


class S3Client:
    def __init__(self):
        self.s3 = None

    def init_app(self, config):
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'],
            region_name=config['AWS_REGION']
        )
        return self.s3
    
s3 = S3Client()