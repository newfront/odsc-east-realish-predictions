# odsc-east-realish-predictions
Material for the 2019 ODSC East Workshop (Realish Time Predictive Analytics with Spark Structured Streaming)

#### Technologies
1. [Spark 2.4.0](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz)
2. [Zeppelin 0.8.1](http://www.apache.org/dyn/closer.cgi/zeppelin/zeppelin-0.8.1/zeppelin-0.8.1-bin-netinst.tgz)
3. [Hadoop 2.7.7](https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz)

#### Spark Local Setup
Just download, untar, and move spark. I usually just drop into usr/local

`tar -xvzf /path/to/spark-2.4.0-bin-hadoop2.7.tgz && mv /path/to/spark-2.4.0-bin-hadoop2.7/ /usr/local/spark-2.4.0/`

#### Zeppelin Setup
[Spark Setup](https://zeppelin.apache.org/docs/0.8.1/interpreter/spark.html)

#### Hadoop Single Node Setup
[Setup Documentation](http://hadoop.apache.org/docs/r2.7.7/hadoop-project-dist/hadoop-common/SingleCluster.html)

#### Local Aliases in .bashrc or .bash_profile
~~~
export SPARK_HOME=/usr/local/spark-2.4.0
export ZEPPELIN_HOME=/usr/local/zeppelin-0.8.1
export HADOOP_HOME=/usr/local/hadoop-2.7.7

# Zeppelin
alias zeppelin_start="$ZEPPELIN_HOME/bin/zeppelin-daemon.sh --config $ZEPPELIN_HOME/conf/ start"
alias zeppelin_stop="$ZEPPELIN_HOME/bin/zeppelin-daemon.sh --config $ZEPPELIN_HOME/conf/ stop"

# Hadoop
alias start_hdfs="$HADOOP_HOME/sbin/start-dfs.sh"
alias stop_hdfs="$HADOOP_HOME/sbin/stop-dfs.sh"
alias hdfs="$HADOOP_HOME/bin/hdfs"
~~~

```
source ~/.bash_profile
```

#### Zeppelin Config (zeppelin-env.sh)
You will need to setup some basic options in the zeppelin-env.sh

`vim /usr/local/zeppelin-0.8.1/conf/zeppelin-env.sh`
~~~bash
/usr/local/zeppelin-0.8.1/conf/zeppelin-env.sh
#### Spark interpreter configuration ####

## Use provided spark installation ##
## defining SPARK_HOME makes Zeppelin run spark interpreter process using spark-submit
##
export SPARK_HOME=/usr/local/spark-2.4.0        # (required) When it is defined, load it instead of Zeppelin embedded Spark libraries
# export SPARK_SUBMIT_OPTIONS                   # (optional) extra options to pass to spark submit. eg) "--driver-memory 512M --executor-memory 1G".
# export SPARK_APP_NAME                         # (optional) The name of spark application.

## Spark interpreter options ##
##
# export ZEPPELIN_SPARK_USEHIVECONTEXT  # Use HiveContext instead of SQLContext if set true. true by default.
# export ZEPPELIN_SPARK_CONCURRENTSQL   # Execute multiple SQL concurrently if set true. false by default.
# export ZEPPELIN_SPARK_IMPORTIMPLICIT  # Import implicits, UDF collection, and sql if set true. true by default.
# export ZEPPELIN_SPARK_MAXRESULT       # Max number of Spark SQL result to display. 1000 by default.
export ZEPPELIN_WEBSOCKET_MAX_TEXT_MESSAGE_SIZE=2048000 # Size in characters of the maximum text message to be received by websocket. Defaults to 1024000
export ZEPPELIN_INTERPRETER_OUTPUT_LIMIT=2048000
~~~

#### Zeppelin Interpreter Spark Settings
1. in the terminal, if you have added the aliases to your bash, `zeppelin_start` - should emit green `[OK]` when running
2. go to http://localhost:8080/#/interpreter
3. under the Spark section, click the edit icon, and add `spark.executor.memory: 6g`, `zeppelin.spark.maxResult: 50000`. It is worth noting that with `spark.cores.max: 4` you will need `24g` of ram to run zeppelin. `spark.cores.max * spark.executor.memory = runtime ram dependency`
4. click `save` and the interpreter will restart with your updated settings.

[Zeppelin Spark Doc](https://zeppelin.apache.org/docs/0.8.1/interpreter/spark.html)
