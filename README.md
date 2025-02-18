Query 100GB Data With AWS S3 Select Under 30 Seconds
===========================================

Amazon S3 Select enables applications to retrieve only a subset of data from an object by using simple SQL expressions. By using S3 Select to retrieve only the data needed by your application, you can achieve drastic performance increases – in many cases, you can get as much as a 400% improvement. [More details](https://aws.amazon.com/blogs/aws/s3-glacier-select/)

This works well when you trying to perform a query against and 5-10 GB of data and getting your result in 1-2 Minutes.

* What if you have to search 100GB of Data.
* What if your application architecture is Serverless.
* What if you have to do this under 30 seconds.

This document will explain to query 100GB Data with s3 select under 30 seconds using AWS Serverless services API Gateway, Lambda, and S3.

Background
==========
I was asked to build an API which allows the user to query 100GB of Data using Serverless services, At the time of writing this document API gateway maximum integration timeout 29 seconds and Lambda has maximum memory of 3GB and two CPU cores.

I created some Benchmarks on S3 Select Data and found if I try to query 100MB data, it usually finished in 1-2 seconds. Bigger the size of Data longer the time of the query.

Problem
=======
How do I query 100GB under 30 seconds?

Solution
========
- Convert data to standard data formats supported by S3 Select support e.g CSV, JSON. 

- Split 100GB data into 100MB chunks and upload to S3 Bucket.

- Create Lambda Function (I choose Python so using Boto3 SDK)

- Create a Boto3 Connection; Increase connection to 1000; default connection is 10

- list S3 files; maximum 1000 keys can be listed from a bucket; Use threads in your code to query 100MB in each thread using S3 Select. 

- You have to do some benchmarking. How many Threads CPU can process in one time; As per Intel Hyperthreading, each core can process 2 Threads. But CPU is not doing much of Job here, we are creating threads to handover to S3 Select to query data and retrieve results. I found 200 threads are good enough and getting my results around 5 seconds; which means I can query now 20GB data in 5 seconds. It's all downhill from here and I was able to query 100GB with 1000 threads and achieve my results under 30 seconds. 

- Attached your Lambda function to API Gateway.

Discussion
==========
This will provide you an overview to understand and implement a solution using S3 select to query a large set of data.

Have you implemented this solution?

Yes, I implemented using serverless architecture and created Lambda function to perform query operations; Added multithreading to query data simultaneously. Attached Lamda function to API Gateway.

Can this be improved further to query a large amount of data?

Lambda concurrency was not used at the moment, but using concurrency I can say we can easily search larger data under 30 seconds.

Is this cost-effective?

Yes, It's cheaper than using Amazon Athena, Amazon Redshift and Amazon EMR. But In the end, it all depends on the amount of data and architecture.
