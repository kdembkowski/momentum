import boto3
import sys
import subprocess
import json
from datetime import datetime
from multiprocessing import Pool
s3_resource = boto3.resource('s3')
s3 = boto3.client('s3')

# parameters
fileExtension = '.mp4'
storageClass = 'STANDARD'
sourceBucket = 'momentum-s3-test-kamil-dembkowski'
destinationBucket = 'momentum-s3-test2-kamil-dembkowski'
pathPrefix = 'store/'
maxPerPageItemsNumber = 1000
numberOfConcurrentlyCopiedFiles = 16


# multiprocessing
def copy_single_file(fileName):
    """
    Helper function to copy a single file
    """
    
    sBucket = 'momentum-s3-test-kamil-dembkowski'
    dBucket = 'momentum-s3-test2-kamil-dembkowski'
       
    try:
        s3.copy_object(
            Bucket = dBucket,
            Key = fileName,
                CopySource = {
                    'Bucket': sBucket,
                    'Key': fileName
                    }
            )
    
    except Exception as e:
        print(f"Error copying {fileName} to {fileName}: {e}")



def copy_files_concurrently(fileNames, numberOfWorkers):
    """
    Function to copy multiple files using multiprocessing for faster execution.
    """
    
    with Pool(processes = numberOfWorkers) as pool:
        pool.map(copy_single_file, fileNames)
    
    print("All files copied successfully.")


#pagination
paginator = s3.get_paginator('list_objects_v2')

for page in paginator.paginate(Bucket = sourceBucket, Prefix = pathPrefix, PaginationConfig = {'PageSize': maxPerPageItemsNumber}):
    
    fileNames = []
    
    if 'Contents' in page:
        for objectS3 in page['Contents']:
            if ((objectS3['Key'].endswith(fileExtension)) and (objectS3['StorageClass'] == storageClass)):
                fileNames.append(objectS3['Key'])
    
    copy_files_concurrently(fileNames, numberOfConcurrentlyCopiedFiles)
    
