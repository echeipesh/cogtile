name := "cogtile"

version := "0.1.0"

description := "Display COG-enabled tiles via TMS."

organization := "com.azavea"

organizationName := "Azavea"

scalaVersion in ThisBuild := "2.11.12"

resolvers ++= Seq(
  "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
  Resolver.bintrayRepo("azavea", "maven")
)

scalacOptions := Seq(
  "-deprecation",
  "-Ypartial-unification",
  "-Ywarn-value-discard",
  "-Ywarn-unused-import",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen"
)

scalacOptions in (Compile, doc) += "-groups"

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-spark"      % "2.0.0-M3",
  "org.locationtech.geotrellis" %% "geotrellis-s3"         % "2.0.0-M3",
  "org.locationtech.geotrellis" %% "geotrellis-vector"     % "2.0.0-M3",
  "org.scalatest"               %% "scalatest"             % "3.0.1" % "test",
  "org.typelevel"               %% "cats-core"             % "1.0.1",
  "org.typelevel"               %% "cats-effect"           % "1.0.0-RC",
  "org.http4s"                  %% "http4s-blaze-server"   % "0.18.10",
  "org.http4s"                  %% "http4s-dsl"            % "0.18.10"
)

parallelExecution in Test := false

val release = Seq(
  bintrayOrganization := Some("azavea"),
  licenses += ("Apache-2.0", url("http://apache.org/licenses/LICENSE-2.0"))
)

