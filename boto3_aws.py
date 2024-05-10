import boto3
from datetime import datetime, timedelta

BUCKET_NAME = "shawan-my-new-bucket"
EXT = ".json"

def assume_role_decorator(role_arn):
    def decorator(func):
        def wrapper(*args, **kwargs):
            #session = boto3.Session(profile_name='AccountDeploymentUser')
            session = boto3.Session(
                    aws_access_key_id='AKIAQKX7MUVIFDW3Z5XS',
                    aws_secret_access_key='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',
                    region_name='us-east-1'
                    )



            sts_client = session.client('sts')
            response = sts_client.assume_role(RoleArn=role_arn, RoleSessionName='AssumeRoleSession')
            
            aws_access_key_id = response['Credentials']['AccessKeyId']
            aws_secret_access_key = response['Credentials']['SecretAccessKey']
            aws_session_token = response['Credentials']['SessionToken']
            
            s3 = boto3.client('s3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token)
            
            return func(s3, *args, **kwargs)
        return wrapper
    return decorator

@assume_role_decorator('arn:aws:iam::023084180816:role/shawan_account_role')
def count_json_objects(s3, start, end):
    # Define the date sequence
    sequence = []
    #current_date = start_date
    #while start <= end:
       # x = current_date.strftime('%Y/%m/%d')
    for i in range(start, end+1):
        sequence.append(f"File{i}")
        #current_date += timedelta(days=1)
    
    # Count JSON objects in S3 bucket
    for d in sequence:
        total_json_objects = 0
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=d):
            objects = page.get('Contents', [])
            json_objects = [obj for obj in objects if obj['Key'].lower().endswith(EXT)]
            total_json_objects += len(json_objects)
        
        if total_json_objects > 0:
            print(f"Total objects in the bucket '{BUCKET_NAME}/{d}': {total_json_objects}")

def main():
    # Define the start and end dates
    #start_date = datetime(2021, 4, 1)  # Replace with start date
    #end_date = datetime(2023, 5, 31)    # Replace with end date

    start = 1
    end = 10
    count_json_objects(start, end)

if __name__ == '__main__':
    main()
