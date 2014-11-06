import AssemblyKeys._ // put this at the top of the file

assemblySettings

name := "hbase-coprocessor"

version := "1.0"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "Central Maven Repo" at "http://repo.maven.apache.org/maven2",
  "Twitter Maven Repository" at "http://maven.twttr.com/",
  "Concurrent Maven Repo" at "http://conjars.org/repo"
)

libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase" % "0.94.18" % "provided",
  "org.apache.hadoop" % "hadoop-core" % "1.0.4" % "provided",
  "com.twitter" %% "algebird-core" % "0.6.0",
  "com.twitter" %% "chill" % "0.3.6",
  "com.twitter" %% "chill-algebird" % "0.3.6",
  "com.twitter" %% "scalding-args" % "0.11.1",
  "org.specs2" %% "specs2" % "2.3.10" % "test"
)

test in assembly := {}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp ⇒
  cp filter { jar ⇒ Set(
      "minlog-1.2.jar"
    )(jar.data.getName)
  }
}

assemblyOption in assembly ~= {
  _.copy(cacheUnzip = false).copy(cacheOutput = false)
}
