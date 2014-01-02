couchbase-test
==============

Test couchbase with create, update, delete of records

# How-to

mvn clean install assembly:single

./runcouchload.sh cluster=couchbase1.mycompany.com:8091,couchbase2.mycompany.com:8091,couchbase3.mycompany.com:8091

(You can replace the cluster option with your own)

# All options with defaults

cluster=localhost:8091
bucketName=test
bucketUser=test
bucketPassword=test
recordCount=10000
recordStart=0
recordDelay=1000
recordStep=1000
readCount=1000
writeCount=500
idPrefix=id-
readDelay=1000
writeDelay=1000
loopCount=10
threadCount=1
removeRecords=0
printResults=0

# Examples

./runcouchload.sh cluster=couchbase1.mycompany.com:8091,couchbase2.mycompany.com:8091 bucketName=test bucketUser=test bucketPassword=test threadCount=100 loopCount=1000 readDelay=100 writeDelay=100
