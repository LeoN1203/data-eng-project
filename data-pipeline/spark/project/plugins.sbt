// Assembly plugin for creating fat JARs
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")
// addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.2.2")

// Docker plugin for containerization
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.8.1")

// Code formatting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

// Dependency updates
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.6.4")

// Git integration
addSbtPlugin("com.github.sbt" % "sbt-git" % "2.0.1")
