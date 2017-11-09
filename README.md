# Enron Email Data
A scala/spark solution to Enron process email data publicly released as part of FERC's Western Energy Markets investigation converted to industry standard formats by EDRM. The data set consists of 
1,227,255 emails with 493,384 attachments covering 151 custodians. The email is provided in Microsoft PST, IETF MIME, and EDRM XML formats.
                          
## What does it do
There are two Spark jobs which analyszes publically avaiable Enron email data

- Average: Calculates the average length in words of emails 
- Top100: Calculated the top 100 emails by recipient email addresses

### How do I build it
This is an Scala/SBT project and it is assumed that SBT has been installed and the instructions which follow uses this assumption

1. Clone the code to your machine
2. Navigate to the top level directory for the source code
3. Issue the following command: ```sbt clean assembly```
4. The above will produce two fat jars which can be found in the following directories:
- {TOP_LEVEL_SOURCE_DIRECTORY}/target/scala-2.11/average-assembly-0.1.0.jar
- {TOP_LEVEL_SOURCE_DIRECTORY}/target/scala-2.11/top100-assembly-0.1.0.jar


## How do I run it on the cluster
The information below assumes that you have an appropriate setup on Amazon AWS 

### Submit job to EMR
```
1. Create a cluster with 3 nodes
2. Add Spark step for all the jobs
3. Upload average-assembly-0.1.0.jar and top100-assembly-0.1.0.jar to a location of your choice on s3

spark-submit --deploy-mode cluster \
             --executor-memory 10g \
             --class co.ioctl.{either average-assembly-0.1.0.jar or top100-assembly-0.1.0.jar} s3://{LOCATION_OF_WHERE_JARS_ARE_STORED} 
```

# TODO
- Add integration tests
- Application is currently only processing a subset of emails, run with entire dataset
- Optimise memory settings
- Update documentation as necessary
