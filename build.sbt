ThisBuild / scalaVersion := "2.11.8"

name:="Milestone1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.1.0"
)

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>

 "Milestone1_" + 309264 + "." + artifact.extension

}

