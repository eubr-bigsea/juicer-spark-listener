name := "juicer-spark-listener"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.sonatypeRepo("public")

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"

libraryDependencies += "org.json4s" % "json4s-native_2.11" % "3.3.0"
