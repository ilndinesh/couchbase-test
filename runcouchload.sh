#!/bin/bash

java -classpath target/couchbase-test-jar-with-dependencies.jar -Xms512m -Xmx2048m -XX:MaxPermSize=256m dev.test.CouchbaseLoadTest $@
