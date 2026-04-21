import json
import boto3
import pymysql
import csv
import io
import os
# from dotenv import load_dotenv

# load_dotenv()

# RDS config
# DB_HOST = "database-1.c9mewsiq2n1k.us-east-1.rds.amazonaws.com"
DB_HOST = os.getenv("host")
DB_USER = os.getenv("user")
# DB_USER = "admin"
# DB_PASSWORD = "admin123"
DB_PASSWORD = os.getenv("pass")
# DB_NAME = "database1"
DB_NAME = os.getenv("database")

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # 1. Get S3 file info
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        print(f"Processing file: {key} from bucket: {bucket}")

        # 2. Read file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')

        csv_data = csv.reader(io.StringIO(content))

        # 3. Connect to RDS
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )

        cursor = conn.cursor()

        # 4. Insert data
        for row in csv_data:
            cursor.execute(
                "INSERT INTO users (id, name, email) VALUES (%s, %s, %s)",
                (row[0], row[1], row[2])
            )

        conn.commit()
        cursor.close()
        conn.close()

        return {
            'statusCode': 200,
            'body': 'Data inserted successfully'
        }

    except Exception as e:
        print("Error:", str(e))
        raise e