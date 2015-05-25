name := "dada"

version := "0.2.0-SNAPSHOT"

organization := "io.nuvo"

homepage :=  Some(new java.net.URL("http://github.com/nuvo-io/dada"))

scalaVersion := "2.11.6"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.6.4"

autoCompilerPlugins := true

scalacOptions += "-deprecation"

scalacOptions += "-unchecked"

scalacOptions += "-optimise"

scalacOptions += "-feature"
