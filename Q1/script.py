import boto3
import pandas as pd
from io import StringIO

# Initialize S3 client
s3 = boto3.client('s3')

# Input details
input_bucket = 'student-etl-heramb'
input_key = 'sales_file.csv'

# Output details
output_bucket = 'student-etl-heramb'
output_key = 'output/filtered-file.csv'

# Step 1: Read file from S3
response = s3.get_object(Bucket=input_bucket, Key=input_key)
csv_content = response['Body'].read().decode('utf-8')

# Step 2: Load into DataFrame
df = pd.read_csv(StringIO(csv_content))

# Step 3: Filter records (amount > 1000)
filtered_df = df[df['amount'] > 1000]

# Step 4: Convert DataFrame back to CSV
csv_buffer = StringIO()
filtered_df.to_csv(csv_buffer, index=False)

# Step 5: Upload back to S3
s3.put_object(
    Bucket=output_bucket,
    Key=output_key,
    Body=csv_buffer.getvalue()
)

print("Filtered file uploaded successfully!")