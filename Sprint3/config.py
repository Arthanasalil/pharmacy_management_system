import os

class Config:
    MYSQL_HOST = os.getenv("MYSQL_HOST", "pharmacy-db.cbuco8icy542.eu-north-1.rds.amazonaws.com")
    MYSQL_USER = os.getenv("MYSQL_USER", "admin")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "admi1234")
    MYSQL_DB = os.getenv("MYSQL_DB", "pharmacy_db")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))

    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
    S3_BUCKET = os.getenv("S3_BUCKET", "")
    S3_PRESCRIPTION_PREFIX =  os.getenv("S3_PRESCRIPTION_PREFIX", "prescriptions")
    
 
    AWS_ACCESS_KEY = AWS_ACCESS_KEY_ID
    AWS_SECRET_KEY = AWS_SECRET_ACCESS_KEY


