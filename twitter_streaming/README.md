# udemy-spark-scala > twitter_streaming

## PopularHashtags.scala

This is a Spark streaming script that monitors live tweets from Twitter and keeps track of the 10 most popular hashtags as tweets are received. 

Each hashtag is mapped to a key/value pair of (hashtag, 1) so they can be counted up over a 5-minute sliding window with this line of code:

```scala
 val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow((x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
```  

#### Sample Output
```
/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/bin/java "-javaagent:/Applications/IntelliJ IDEA CE.app/Contents/lib/idea_rt.jar=55524:/Applications/IntelliJ IDEA CE.app/Contents/bin" -Dfile.encoding=UTF-8 -classpath /Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/charsets.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/deploy.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/ext/cldrdata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/ext/dnsns.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/ext/jaccess.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/ext/jfxrt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/ext/localedata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/ext/nashorn.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/ext/sunec.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/ext/sunjce_provider.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/ext/sunpkcs11.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/ext/zipfs.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/javaws.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/jce.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/jfr.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/jfxswt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/jsse.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/management-agent.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/plugin.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/resources.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/jre/lib/rt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/lib/ant-javafx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/lib/dt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/lib/javafx-mx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/lib/jconsole.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/lib/packager.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/lib/sa-jdi.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/lib/tools.jar:/Users/andgoss/GitHub_AG/udemy-spark-scala/twitter_streaming/target/scala-2.11/classes:/Users/andgoss/.ivy2/cache/org.scala-lang/scala-library/jars/scala-library-2.11.8.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/activation-1.1.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/antlr-2.7.7.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/antlr-runtime-3.4.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/antlr4-runtime-4.5.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/aopalliance-1.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/aopalliance-repackaged-2.4.0-b34.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/apache-log4j-extras-1.2.17.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/apacheds-i18n-2.0.0-M15.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/apacheds-kerberos-codec-2.0.0-M15.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/api-asn1-api-1.0.0-M20.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/api-util-1.0.0-M20.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/arpack_combined_all-0.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/avro-1.7.7.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/avro-ipc-1.7.7.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/avro-mapred-1.7.7-hadoop2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/base64-2.3.8.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/bcprov-jdk15on-1.51.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/bonecp-0.8.0.RELEASE.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/breeze_2.11-0.13.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/breeze-macros_2.11-0.13.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/calcite-avatica-1.2.0-incubating.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/calcite-core-1.2.0-incubating.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/calcite-linq4j-1.2.0-incubating.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/chill_2.11-0.8.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/chill-java-0.8.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-beanutils-1.7.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-beanutils-core-1.8.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-cli-1.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-codec-1.10.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-collections-3.2.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-compiler-3.0.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-compress-1.4.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-configuration-1.6.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-crypto-1.0.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-dbcp-1.4.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-digester-1.8.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-httpclient-3.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-io-2.4.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-lang-2.6.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-lang3-3.5.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-logging-1.1.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-math3-3.4.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-net-2.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/commons-pool-1.5.4.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/compress-lzf-1.0.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/core-1.1.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/curator-client-2.6.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/curator-framework-2.6.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/curator-recipes-2.6.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/datanucleus-api-jdo-3.2.6.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/datanucleus-core-3.2.10.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/datanucleus-rdbms-3.2.9.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/derby-10.12.1.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/eigenbase-properties-1.1.5.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/gson-2.2.4.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/guava-14.0.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/guice-3.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/guice-servlet-3.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-annotations-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-auth-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-common-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-hdfs-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-mapreduce-client-app-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-mapreduce-client-common-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-mapreduce-client-core-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-mapreduce-client-jobclient-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-mapreduce-client-shuffle-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-yarn-api-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-yarn-client-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-yarn-common-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-yarn-server-common-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hadoop-yarn-server-web-proxy-2.7.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hive-beeline-1.2.1.spark2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hive-cli-1.2.1.spark2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hive-exec-1.2.1.spark2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hive-jdbc-1.2.1.spark2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hive-metastore-1.2.1.spark2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hk2-api-2.4.0-b34.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hk2-locator-2.4.0-b34.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/hk2-utils-2.4.0-b34.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/htrace-core-3.1.0-incubating.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/httpclient-4.5.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/httpcore-4.4.4.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/ivy-2.4.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jackson-annotations-2.6.5.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jackson-core-2.6.5.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jackson-core-asl-1.9.13.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jackson-databind-2.6.5.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jackson-jaxrs-1.9.13.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jackson-mapper-asl-1.9.13.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jackson-module-paranamer-2.6.5.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jackson-module-scala_2.11-2.6.5.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jackson-xc-1.9.13.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/janino-3.0.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/java-xmlbuilder-1.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/JavaEWAH-0.3.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/javassist-3.18.1-GA.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/javax.annotation-api-1.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/javax.inject-1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/javax.inject-2.4.0-b34.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/javax.servlet-api-3.1.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/javax.ws.rs-api-2.0.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/javolution-5.5.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jaxb-api-2.2.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jcl-over-slf4j-1.7.16.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jdo-api-3.0.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jersey-client-2.22.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jersey-common-2.22.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jersey-container-servlet-2.22.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jersey-container-servlet-core-2.22.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jersey-guava-2.22.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jersey-media-jaxb-2.22.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jersey-server-2.22.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jets3t-0.9.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jetty-6.1.26.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jetty-util-6.1.26.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jline-2.12.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/joda-time-2.9.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jodd-core-3.5.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jpam-1.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/json4s-ast_2.11-3.2.11.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/json4s-core_2.11-3.2.11.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/json4s-jackson_2.11-3.2.11.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jsp-api-2.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jsr305-1.3.9.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jta-1.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jtransforms-2.4.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/jul-to-slf4j-1.7.16.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/kryo-shaded-3.0.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/leveldbjni-all-1.8.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/libfb303-0.9.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/libthrift-0.9.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/log4j-1.2.17.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/lz4-1.3.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/machinist_2.11-0.6.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/macro-compat_2.11-1.1.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/mail-1.4.7.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/mesos-1.0.0-shaded-protobuf.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/metrics-core-3.1.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/metrics-graphite-3.1.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/metrics-json-3.1.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/metrics-jvm-3.1.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/minlog-1.3.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/mx4j-3.0.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/netty-3.9.9.Final.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/netty-all-4.0.43.Final.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/objenesis-2.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/opencsv-2.3.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/oro-2.0.8.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/osgi-resource-locator-1.0.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/paranamer-2.6.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/parquet-column-1.8.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/parquet-common-1.8.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/parquet-encoding-1.8.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/parquet-format-2.3.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/parquet-hadoop-1.8.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/parquet-hadoop-bundle-1.6.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/parquet-jackson-1.8.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/pmml-model-1.2.15.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/pmml-schema-1.2.15.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/protobuf-java-2.5.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/py4j-0.10.4.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/pyrolite-4.13.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/RoaringBitmap-0.5.11.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/scala-compiler-2.11.8.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/scala-library-2.11.8.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/scala-parser-combinators_2.11-1.0.4.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/scala-reflect-2.11.8.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/scala-xml_2.11-1.0.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/scalap-2.11.8.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/shapeless_2.11-2.3.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/slf4j-api-1.7.16.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/slf4j-log4j12-1.7.16.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/snappy-0.2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/snappy-java-1.1.2.6.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-catalyst_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-core_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-graphx_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-hive_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-hive-thriftserver_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-launcher_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-mesos_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-mllib_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-mllib-local_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-network-common_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-network-shuffle_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-repl_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-sketch_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-sql_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-streaming_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-tags_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-unsafe_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spark-yarn_2.11-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spire_2.11-0.13.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/spire-macros_2.11-0.13.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/ST4-4.0.4.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/stax-api-1.0-2.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/stax-api-1.0.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/stream-2.7.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/stringtemplate-3.2.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/super-csv-2.2.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/univocity-parsers-2.2.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/validation-api-1.1.0.Final.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/xbean-asm5-shaded-4.4.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/xercesImpl-2.9.1.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/xmlenc-0.52.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/xz-1.0.jar:/opt/spark-2.2.0-bin-hadoop2.7/jars/zookeeper-3.4.6.jar:/Users/andgoss/Downloads/SparkScala/dstream-twitter_2.11-0.1.0-SNAPSHOT.jar:/Users/andgoss/Downloads/SparkScala/twitter4j-core-4.0.4.jar:/Users/andgoss/Downloads/SparkScala/twitter4j-stream-4.0.4.jar com.andrewrgoss.spark.PopularHashtags
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
18/03/21 11:05:05 INFO SparkContext: Running Spark version 2.2.0
18/03/21 11:05:06 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/03/21 11:05:06 WARN Utils: Your hostname, usbosrad3116.global.publicisgroupe.net resolves to a loopback address: 127.0.0.1; using 192.168.1.105 instead (on interface en0)
18/03/21 11:05:06 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
18/03/21 11:05:06 INFO SparkContext: Submitted application: PopularHashtags
18/03/21 11:05:06 INFO SecurityManager: Changing view acls to: andgoss
18/03/21 11:05:06 INFO SecurityManager: Changing modify acls to: andgoss
18/03/21 11:05:06 INFO SecurityManager: Changing view acls groups to: 
18/03/21 11:05:06 INFO SecurityManager: Changing modify acls groups to: 
18/03/21 11:05:06 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(andgoss); groups with view permissions: Set(); users  with modify permissions: Set(andgoss); groups with modify permissions: Set()
18/03/21 11:05:07 INFO Utils: Successfully started service 'sparkDriver' on port 55527.
18/03/21 11:05:07 INFO SparkEnv: Registering MapOutputTracker
18/03/21 11:05:07 INFO SparkEnv: Registering BlockManagerMaster
18/03/21 11:05:07 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/03/21 11:05:07 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/03/21 11:05:07 INFO DiskBlockManager: Created local directory at /private/var/folders/gp/bcfcnndd1r560tcdwqpqr4t5ysdjsk/T/blockmgr-7bc076c8-378f-4be4-83b7-9ee59feac8a7
18/03/21 11:05:07 INFO MemoryStore: MemoryStore started with capacity 2004.6 MB
18/03/21 11:05:07 INFO SparkEnv: Registering OutputCommitCoordinator
18/03/21 11:05:07 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/03/21 11:05:07 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.1.105:4040
18/03/21 11:05:08 INFO Executor: Starting executor ID driver on host localhost
18/03/21 11:05:08 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 55528.
18/03/21 11:05:08 INFO NettyBlockTransferService: Server created on 192.168.1.105:55528
18/03/21 11:05:08 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/03/21 11:05:08 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.1.105, 55528, None)
18/03/21 11:05:08 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.1.105:55528 with 2004.6 MB RAM, BlockManagerId(driver, 192.168.1.105, 55528, None)
18/03/21 11:05:08 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.1.105, 55528, None)
18/03/21 11:05:08 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.1.105, 55528, None)

-------------------------------------------
Time: 1521644713000 ms
-------------------------------------------
(#春だしロードスター乗りのお友達を増やそうキャンペーン
紛れもなくロードスターではある
…
ロードスター。,1)
(#productivity!,1)
(#赖冠霖,1)
(#บุพเพสันนิวาส,1)
(#워너원,1)
(#legranddirect,1)
(#BTSArmy!,1)
(#ToYourSuccess,1)
(#โป๊ปธนวรรธน์,1)
(#GOT7,1)
...

-------------------------------------------
Time: 1521644714000 ms
-------------------------------------------
(#บุพเพสันนิวาส,2)
(#Ankara/Esenboğa,1)
(#YONGJUNHYUNG,1)
(#10cm,1)
(#春だしロードスター乗りのお友達を増やそうキャンペーン
紛れもなくロードスターではある
…
ロードスター。,1)
(#productivity!,1)
(#Education…,1)
(#Türkiye,1)
(#赖冠霖,1)
(#워너원,1)
...

-------------------------------------------
Time: 1521644715000 ms
-------------------------------------------
(#บุพเพสันนิวาส,2)
(#GOT7,2)
(#GalaxyGift,2)
(#Ankara/Esenboğa,1)
(#YONGJUNHYUNG,1)
(#10cm,1)
(#오고싶당
#아직춥당,1)
(#春だしロードスター乗りのお友達を増やそうキャンペーン
紛れもなくロードスターではある
…
ロードスター。,1)
(#WorldPoetryDay,,1)
(#yrittäjyys,1)
...

-------------------------------------------
Time: 1521644716000 ms
-------------------------------------------
(#からだすこやか茶W,2)
(#บุพเพสันนิวาส,2)
(#GOT7,2)
(#GalaxyGift,2)
(#Ankara/Esenboğa,1)
(#FdI,1)
(#RetailForum2018,1)
(#YONGJUNHYUNG,1)
(#10cm,1)
(#오고싶당
#아직춥당,1)
...

-------------------------------------------
Time: 1521644717000 ms
-------------------------------------------
(#からだすこやか茶W,3)
(#บุพเพสันนิวาส,3)
(#GOT7,2)
(#GalaxyGift,2)
(#OliviaHye
LOOΠΔ
https://t.co/5CfbXI886V,1)
(#Ankara/Esenboğa,1)
(#FdI,1)
(#RetailForum2018,1)
(#YONGJUNHYUNG,1)
(#10cm,1)
...

-------------------------------------------
Time: 1521644718000 ms
-------------------------------------------
(#บุพเพสันนิวาส,5)
(#からだすこやか茶W,3)
(#GOT7,2)
(#المشروع_الوطني_تعليم_الخرج١٩

١٤٠٦...,2)
(#GalaxyGift,2)
(#OliviaHye
LOOΠΔ
https://t.co/5CfbXI886V,1)
(#Ankara/Esenboğa,1)
(#สวนดงโฮ,1)
(#FdI,1)
(#RetailForum2018,1)
...

-------------------------------------------
Time: 1521644719000 ms
-------------------------------------------
(#บุพเพสันนิวาส,5)
(#からだすこやか茶W,3)
(#GOT7,2)
(#المشروع_الوطني_تعليم_الخرج١٩

١٤٠٦...,2)
(#GalaxyGift,2)
(#OliviaHye
LOOΠΔ
https://t.co/5CfbXI886V,1)
(#Ankara/Esenboğa,1)
(#สวนดงโฮ,1)
(#FdI,1)
(#RetailForum2018,1)
...

-------------------------------------------
Time: 1521644720000 ms
-------------------------------------------
(#บุพเพสันนิวาส,6)
(#からだすこやか茶W,3)
(#GOT7,2)
(#المشروع_الوطني_تعليم_الخرج١٩

١٤٠٦...,2)
(#GalaxyGift,2)
(#ドラゴンプロジェクト,1)
(#OliviaHye
LOOΠΔ
https://t.co/5CfbXI886V,1)
(#Ankara/Esenboğa,1)
(#สวนดงโฮ,1)
(#Weathercloud,1)
...

-------------------------------------------
Time: 1521644721000 ms
-------------------------------------------
(#บุพเพสันนิวาส,7)
(#からだすこやか茶W,3)
(#GOT7,2)
(#المشروع_الوطني_تعليم_الخرج١٩

١٤٠٦...,2)
(#GalaxyGift,2)
(#محمد_بن_سلمان,1)
(#ولي_العهد,1)
(#ドラゴンプロジェクト,1)
(#OliviaHye
LOOΠΔ
https://t.co/5CfbXI886V,1)
(#Ankara/Esenboğa,1)
...

-------------------------------------------
Time: 1521644722000 ms
-------------------------------------------
(#บุพเพสันนิวาส,8)
(#からだすこやか茶W,3)
(#GOT7,3)
(#المشروع_الوطني_تعليم_الخرج١٩

١٤٠٦...,2)
(#동방신기,2)
(#GalaxyGift,2)
(#THE_CHANCE_OF_LOVE,1)
(#محمد_بن_سلمان,1)
(#ولي_العهد,1)
(#WorldPoetryDay,1)
...

-------------------------------------------
Time: 1521644723000 ms
-------------------------------------------
(#บุพเพสันนิวาส,10)
(#GOT7,4)
(#からだすこやか茶W,3)
(#JTMA2018,2)
(#المشروع_الوطني_تعليم_الخرج١٩

١٤٠٦...,2)
(#동방신기,2)
(#GalaxyGift,2)
(#THE_CHANCE_OF_LOVE,1)
(#محمد_بن_سلمان,1)
(#fightracism,1)
...

-------------------------------------------
Time: 1521644724000 ms
-------------------------------------------
(#บุพเพสันนิวาส,10)
(#GOT7,4)
(#からだすこやか茶W,3)
(#JTMA2018,2)
(#동방신기,2)
(#المشروع_الوطني_تعليم_الخرج١٩

١٤٠٦...,2)
(#GalaxyGift,2)
(#THE_CHANCE_OF_LOVE,1)
(#محمد_بن_سلمان,1)
(#fightracism,1)
...

-------------------------------------------
Time: 1521644725000 ms
-------------------------------------------
(#บุพเพสันนิวาส,13)
(#GOT7,4)
(#からだすこやか茶W,3)
(#JTMA2018,2)
(#동방신기,2)
(#المشروع_الوطني_تعليم_الخرج١٩

١٤٠٦...,2)
(#GalaxyGift,2)
(#THE_CHANCE_OF_LOVE,1)
(#محمد_بن_سلمان,1)
(#fightracism,1)
...

-------------------------------------------
Time: 1521644726000 ms
-------------------------------------------
(#บุพเพสันนิวาส,13)
(#GOT7,4)
(#からだすこやか茶W,3)
(#WorldPoetryDay,2)
(#JTMA2018,2)
(#동방신기,2)
(#المشروع_الوطني_تعليم_الخرج١٩

١٤٠٦...,2)
(#GalaxyGift,2)
(#눈을_떠보니_새하얀_방이었다로_시작하는_글쓰기
눈을,1)
(#fightracism,1)
...

-------------------------------------------
Time: 1521644727000 ms
-------------------------------------------
(#บุพเพสันนิวาส,13)
(#GOT7,4)
(#からだすこやか茶W,3)
(#WorldPoetryDay,2)
(#got7,2)
(#JTMA2018,2)
(#동방신기,2)
(#المشروع_الوطني_تعليم_الخرج١٩

١٤٠٦...,2)
(#GalaxyGift,2)
(#눈을_떠보니_새하얀_방이었다로_시작하는_글쓰기
눈을,1)
...

-------------------------------------------
Time: 1521644728000 ms
-------------------------------------------
(#บุพเพสันนิวาส,14)
(#GOT7,5)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#동방신기,3)
(#GalaxyGift,3)
(#WorldPoetryDay,2)
(#워너원,2)
(#got7,2)
(#라이관린,2)
...

-------------------------------------------
Time: 1521644729000 ms
-------------------------------------------
(#บุพเพสันนิวาส,14)
(#GOT7,5)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#라이관린,3)
(#동방신기,3)
(#GalaxyGift,3)
(#WorldPoetryDay,2)
(#하성운,2)
(#FavMusicalGroupTwentyOnePilots,2)
...

-------------------------------------------
Time: 1521644730000 ms
-------------------------------------------
(#บุพเพสันนิวาส,15)
(#GOT7,5)
(#동방신기,4)
(#からだすこやか茶W,3)
(#워너원,3)
(#JTMA2018,3)
(#라이관린,3)
(#GalaxyGift,3)
(#WorldPoetryDay,2)
(#U_Know,2)
...

-------------------------------------------
Time: 1521644731000 ms
-------------------------------------------
(#บุพเพสันนิวาส,15)
(#GOT7,5)
(#동방신기,4)
(#からだすこやか茶W,3)
(#워너원,3)
(#JTMA2018,3)
(#라이관린,3)
(#GalaxyGift,3)
(#WorldPoetryDay,2)
(#U_Know,2)
...

-------------------------------------------
Time: 1521644732000 ms
-------------------------------------------
(#บุพเพสันนิวาส,16)
(#GOT7,5)
(#워너원,4)
(#동방신기,4)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#라이관린,3)
(#GalaxyGift,3)
(#WorldPoetryDay,2)
(#BTSxYouTubeRed,2)
...

-------------------------------------------
Time: 1521644733000 ms
-------------------------------------------
(#บุพเพสันนิวาส,16)
(#GOT7,5)
(#워너원,4)
(#동방신기,4)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#라이관린,3)
(#GalaxyGift,3)
(#WorldPoetryDay,2)
(#BTSxYouTubeRed,2)
...

-------------------------------------------
Time: 1521644734000 ms
-------------------------------------------
(#บุพเพสันนิวาส,19)
(#GOT7,6)
(#워너원,4)
(#동방신기,4)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#라이관린,3)
(#GalaxyGift,3)
(#WorldPoetryDay,2)
(#BTSxYouTubeRed,2)
...

-------------------------------------------
Time: 1521644735000 ms
-------------------------------------------
(#บุพเพสันนิวาส,19)
(#GOT7,6)
(#워너원,4)
(#동방신기,4)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#라이관린,3)
(#GalaxyGift,3)
(#WorldPoetryDay,2)
(#BTSxYouTubeRed,2)
...

-------------------------------------------
Time: 1521644736000 ms
-------------------------------------------
(#บุพเพสันนิวาส,20)
(#GOT7,6)
(#동방신기,5)
(#워너원,4)
(#FavMusicalGroupTwentyOnePilots,3)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#KCA,3)
(#GalaxyGift,3)
(#라이관린,3)
...

-------------------------------------------
Time: 1521644737000 ms
-------------------------------------------
(#บุพเพสันนิวาส,20)
(#GOT7,6)
(#동방신기,5)
(#워너원,4)
(#FavMusicalGroupTwentyOnePilots,3)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#KCA,3)
(#GalaxyGift,3)
(#라이관린,3)
...

-------------------------------------------
Time: 1521644738000 ms
-------------------------------------------
(#บุพเพสันนิวาส,20)
(#GOT7,6)
(#동방신기,5)
(#워너원,4)
(#FavMusicalGroupTwentyOnePilots,3)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#KCA,3)
(#GalaxyGift,3)
(#라이관린,3)
...

-------------------------------------------
Time: 1521644739000 ms
-------------------------------------------
(#บุพเพสันนิวาส,21)
(#GOT7,6)
(#워너원,5)
(#동방신기,5)
(#FavMusicalGroupTwentyOnePilots,3)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#KCA,3)
(#GalaxyGift,3)
(#라이관린,3)
...

-------------------------------------------
Time: 1521644740000 ms
-------------------------------------------
(#บุพเพสันนิวาส,21)
(#GOT7,6)
(#워너원,5)
(#동방신기,5)
(#FavMusicalGroupTwentyOnePilots,3)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#KCA,3)
(#GalaxyGift,3)
(#라이관린,3)
...

-------------------------------------------
Time: 1521644741000 ms
-------------------------------------------
(#บุพเพสันนิวาส,22)
(#GOT7,6)
(#워너원,5)
(#GalaxyGift,5)
(#동방신기,5)
(#FavMusicalGroupTwentyOnePilots,3)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#KCA,3)
(#라이관린,3)
...

-------------------------------------------
Time: 1521644742000 ms
-------------------------------------------
(#บุพเพสันนิวาส,25)
(#GOT7,6)
(#워너원,5)
(#GalaxyGift,5)
(#동방신기,5)
(#KCA,4)
(#FavMusicalGroupTwentyOnePilots,3)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#라이관린,3)
...

-------------------------------------------
Time: 1521644743000 ms
-------------------------------------------
(#บุพเพสันนิวาส,25)
(#GOT7,6)
(#워너원,5)
(#GalaxyGift,5)
(#동방신기,5)
(#KCA,4)
(#FavMusicalGroupTwentyOnePilots,3)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#라이관린,3)
...

18/03/21 11:05:44 ERROR ReceiverTracker: Deregistered receiver for stream 0: Restarting receiver with delay 2000ms: Error receiving tweets - java.util.concurrent.RejectedExecutionException: Task twitter4j.StatusStreamBase$1@7fa9614c rejected from java.util.concurrent.ThreadPoolExecutor@7a4e06c[Terminated, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 2005]
	at java.util.concurrent.ThreadPoolExecutor$AbortPolicy.rejectedExecution(ThreadPoolExecutor.java:2063)
	at java.util.concurrent.ThreadPoolExecutor.reject(ThreadPoolExecutor.java:830)
	at java.util.concurrent.ThreadPoolExecutor.execute(ThreadPoolExecutor.java:1379)
	at twitter4j.DispatcherImpl.invokeLater(DispatcherImpl.java:58)
	at twitter4j.StatusStreamBase.handleNextElement(StatusStreamBase.java:80)
	at twitter4j.StatusStreamImpl.next(StatusStreamImpl.java:56)
	at twitter4j.TwitterStreamImpl$TwitterStreamConsumer.run(TwitterStreamImpl.java:568)

-------------------------------------------
Time: 1521644744000 ms
-------------------------------------------
(#บุพเพสันนิวาส,25)
(#GOT7,6)
(#워너원,5)
(#GalaxyGift,5)
(#동방신기,5)
(#KCA,4)
(#FavMusicalGroupTwentyOnePilots,3)
(#からだすこやか茶W,3)
(#JTMA2018,3)
(#라이관린,3)
...

Exception in thread "receiver-supervisor-future-0" java.lang.Error: java.lang.InterruptedException: sleep interrupted
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1155)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
Caused by: java.lang.InterruptedException: sleep interrupted
	at java.lang.Thread.sleep(Native Method)
	at org.apache.spark.streaming.receiver.ReceiverSupervisor$$anonfun$restartReceiver$1.apply$mcV$sp(ReceiverSupervisor.scala:196)
	at org.apache.spark.streaming.receiver.ReceiverSupervisor$$anonfun$restartReceiver$1.apply(ReceiverSupervisor.scala:189)
	at org.apache.spark.streaming.receiver.ReceiverSupervisor$$anonfun$restartReceiver$1.apply(ReceiverSupervisor.scala:189)
	at scala.concurrent.impl.Future$PromiseCompletingRunnable.liftedTree1$1(Future.scala:24)
	at scala.concurrent.impl.Future$PromiseCompletingRunnable.run(Future.scala:24)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	... 2 more

Process finished with exit code 130 (interrupted by signal 2: SIGINT)
```

#### A Few Takeaways

* This exercise serves as a reminder that English is not as dominant of a language as we tend to think here in America.
* GOT7 is not actually referring to Game of Thrones season 7 as I initially thought - it's actually a South Korean boy band: https://en.wikipedia.org/wiki/Got7.
* The top trending topic \#บุพเพสันนิวาส translates to 'Bubbly' (https://goo.gl/vEK1fx), which is a (clearly popular) television drama series aired in Thailand.


#### Other Self-Challenges
* Some ideas to try:
    * Most popular words tweeted in general
    * Most popular tweet length