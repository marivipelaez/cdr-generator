name := "CDR-Generator"

version := "1.0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0" withSources() withJavadoc()

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.0.0"withSources() withJavadoc()

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.2.0" withSources() withJavadoc()

libraryDependencies += "joda-time" % "joda-time" % "2.3" withSources() withJavadoc()

libraryDependencies += "org.joda" % "joda-convert" % "1.2" withSources() withJavadoc()

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test" withSources() withJavadoc()

libraryDependencies += "org.scalanlp" %% "breeze" % "0.10" withSources() withJavadoc()

libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.10" withSources() withJavadoc()

scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value+"/README.md")

scalacOptions in (Compile, doc) ++= Seq("-doc-title", "CDR Generator")

resolvers += "Akka Repository" at "http://repo.akka.io/release/"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
