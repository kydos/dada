name := "dada"

version := "0.1"

organization := "net.icorsaro"

homepage :=  Some(new java.net.URL("http://github.com/kydos/dada"))

scalaVersion := "2.9.2"

resolvers += "Local Ivy2 Repo" at "file://"+Path.userHome.absolutePath+"/.ivy2/local"


libraryDependencies += "org.scala-lang" % "scala-swing" % "2.9.2"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.6.4"

libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.6.4"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.1"

libraryDependencies += "net.icorsaro" % "escalier_2.9.2" % "0.4.2"

autoCompilerPlugins := true

scalacOptions += "-deprecation"

scalacOptions += "-unchecked"

scalacOptions += "-optimise"
