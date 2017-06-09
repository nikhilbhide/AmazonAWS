## Find Duplicate Files in Amazon AWS S3 using Apache Spark

Amazon AWS S3 (Simple Storage Service) is a object storage service from Amazon and its used to store different objects in buckets and users can perform CRUD operations on thpse objects using a simple web service interface.  Many apps, tools and S3 clients exist to access s3 easily; however, none of them has a feature to identify duplicate files in S3 (or atleast I could not find any such app/tool). Ofcourse, using AWS command line tools you can do that by comparing etag value of all objects, but it really depends on how object was created (multipart upload) and how object was encrypted. This project is to find duplicate files in AWS S3 using Apache Spark. So, you can not totally rely upon aws command line tool results.
We need to perform following steps -
- Access S3 for which AWS credentials are required.
- Explore S3 and identify different file paths. Now this exploration can be either be for selected buckets or for entire s3 account.
- For comparison of files, we need some common basis, which is checksum of file. This is calcumated by computing md5 of file contents.
- Group files based on checksum and then identify duplicate files having same checksum.

### How to Run?
#### With Scala-Eclipse
Use mvn eclipse:eclipse command to create eclipse project settings and then import project into IDE. Create a new Run Configuration and provide required arguments (AWS_ACCESS_KEY) and (AWS_SECRET_KEY) as command line arguments before running the S3FindDuplicateFilesTest. Optionally, you can provide comma separated bucket names as third argument.

#### With Spark-Submit
Create fat-jar using mvn package command and run spark-submit.

.bin/spark-submit \
--class com.nik.spark.S3FindDuplicateFilesTest \
<jar_location> \
<AWS_ACCESSKEY> <AWS_SECRET_KEY> [bucket1,bucket2...]

#Note - You need Hadoop to run application using Spark-Submit.

#### With AWS EMR
If you dont have local setup of Spark then you can application on AWS EMR. Generate the jar using mvn package and then create AWS EMR cluster (small one with one master and 2 workers is more than enough). Upload jar file to s3 bucket and then log in on to EMR EC2 instance and run spark submit as described above. 
