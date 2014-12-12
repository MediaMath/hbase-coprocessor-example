hbase-coprocessor-example
=================
This example demonstrates how to implement a group-by aggregation using HBase coprocessor and Algebird monoid. The HBase version we used here is 
0.94.18, which is exactly the same one available on AWS EMR.

### Create a demo table in your local HBase application
* Download and unzip hbase 0.94.18
* Start hbase ```bin/start-hbase.sh``` 
* Create a table named as mobile-device
```shell
create 'mobile-device', { NAME => 'stats', VERSIONS => 1, TTL => 7776000 }
``` 

### Build and deploy the coprocessor demo code
* Compile a fat jar, and copy it to HBase classpath
```shell
$ sbt assembly
$ cp $PWD/target/scala-2.10/hbase-coprocessor-assembly-1.0.jar $HBASE_DIR/lib/
```
* Edit $HBASE_HOME/conf/hbase-site.xml to include coprocessor class in the configuration
```xml
<property>
    <name>hbase.coprocessor.region.classes</name>
    <value>GroupByMonoidSumCoprocessorEndpoint</value>
</property>
```
* Restart HBase
```shell
$ $HBASE_HOME/bin/hbase-stop.sh 
$ $HBASE_HOME/bin/hbase-start.sh
```

### Run tests
```shell 
$ sbt test
```
