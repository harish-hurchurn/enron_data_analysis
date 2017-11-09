import EnronDataAnalysis._

name := "Enron_Data_Analysis"

lazy val enronDataAnalysis = (project in file("."))
  .settings(EnronDataAnalysis.settings: _*)
  .aggregate(enronAverageWordCountFromEmail)
  .aggregate(enronTopOneHundred)

val enronAverageWordCountFromEmail = (project in file("average"))
  .settings(EnronDataAnalysis.settings: _*)
  .settings(
    name := "average",
    libraryDependencies ++= EnronDataAnalysis.Dependencies.dependencies,
    version := "0.1.0"
  )

val enronTopOneHundred = (project in file("top100"))
  .settings(EnronDataAnalysis.settings: _*)
  .settings(
    name := "top100",
    libraryDependencies ++= EnronDataAnalysis.Dependencies.dependencies,
    version := "0.1.0"
  )