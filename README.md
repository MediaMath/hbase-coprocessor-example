hbase-coprocessor-example
=================
This example demonstrate how to implement group by aggregation using HBase coprocessor. The HBase version used here is 
0.94.18, which is exactly the same version used in AWS EMR.

### Create demo table in local HBase
* Download and unzip hbase 0.94.18
* Start hbase ```bin/start-hbase.sh``` 
* Create the table for unit tests
```shell
create 'mobile-device', { NAME => 'stats', VERSIONS => 1, TTL => 7776000 }
``` 

### Build and deploy coprocessor demo code
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
The tests loads the example dataset in data/mobile_device_samples.tsv to HBase and query "impressions" and "uniques" 
group on "device" field for campaign "1".
