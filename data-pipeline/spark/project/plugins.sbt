// SBT Native Packager for creating distributables and Docker images
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.16")

// Assembly plugin for creating fat JARs (alternative approach)
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")

// Scalafmt for code formatting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
