# udemy-spark-scala >> advanced_examples

To compile a .jar file in IntelliJ IDEA with SBT, use the sbt shell 'package' command:

```
/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home/bin/java -agentlib:jdwp=transport=dt_socket,address=localhost:51183,suspend=n,server=y -Xdebug -server -Xmx768M -XX:MaxPermSize=384M -Dfile.encoding=UTF-8 -Didea.runid=2017.2 -Didea.managed=true -jar "/Users/andgoss/Library/Application Support/IdeaIC2017.3/Scala/launcher/sbt-launch.jar" idea-shell
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=384M; support was removed in 8.0
Listening for transport dt_socket at address: 51183
[info] Loading settings from idea.sbt ...
[info] Loading global plugins from /Users/andgoss/.sbt/1.0/plugins
[info] Loading project definition from /Users/andgoss/GitHub_AG/udemy-spark-scala/advanced_examples/project
[info] Loading settings from build.sbt ...
[info] Set current project to advanced_examples (in build file:/Users/andgoss/GitHub_AG/udemy-spark-scala/advanced_examples/)
[IJ]sbt:advanced_examples> package
```

If this fails with errors 'Unknown artifact. Not resolved or indexed', visit SO to debug:

https://stackoverflow.com/questions/41372978/unknown-artifact-not-resolved-or-indexed-error-for-scalatest

build.sbt requires this dependency information:

```
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1"
```

https://index.scala-lang.org/apache/spark/spark-core/2.2.1?target=_2.11

To run:

```
:advanced_examples andgoss$ spark-submit --class com.andrewrgoss.spark.MovieSimilarities target/scala-2.11/advanced_examples_2.11-0.1.jar 50
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties

Loading movie names...
                                                                                
Top 10 similar movies for Star Wars (1977)
Empire Strikes Back, The (1980) score: 0.9895522078385338       strength: 345
Return of the Jedi (1983)       score: 0.9857230861253026       strength: 480
Raiders of the Lost Ark (1981)  score: 0.981760098872619        strength: 380
20,000 Leagues Under the Sea (1954)     score: 0.9789385605497993       strength: 68
12 Angry Men (1957)     score: 0.9776576120448436       strength: 109
Close Shave, A (1995)   score: 0.9775948291054827       strength: 92
African Queen, The (1951)       score: 0.9764692222674887       strength: 138
Sting, The (1973)       score: 0.9751512937740359       strength: 204
Wrong Trousers, The (1993)      score: 0.9748681355460885       strength: 103
Wallace & Gromit: The Best of Aardman Animation (1996)  score: 0.9741816128302572       strength: 58
```