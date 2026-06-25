scalaVersion := "2.13.13"

lazy val root = rootProject
  .settings(
    name := "estudos-spark-scala",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.1"
    )
  )
