
import boto3

s3 = boto3.client('s3')

# parameters - move files from one bucket to another
fileExtension = '.mp4'
storageClass = 'STANDARD'
#pathPrefix = '/store/'
sourceBucket = 'momentum-s3-test-kamil-dembkowski'
destinationBucket = 'momentum-s3-test2-kamil-dembkowski'

for key in s3.list_objects(Bucket = sourceBucket)['Contents']:
     if ((key['Key'].endswith(fileExtension)) and (key['StorageClass'] == storageClass)):
          s3.copy_object(
               Bucket = destinationBucket,
               Key = key['Key'],
                    CopySource = {
                         'Bucket': sourceBucket,
                         'Key': key['Key']
                    }
               )

