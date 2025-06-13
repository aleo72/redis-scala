addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")
//import sbtassembly.AssemblyPlugin.autoImport._
//import sbtassembly.MergeStrategy
//
//assembly / assemblyMergeStrategy := {
//  case PathList("module-info.class") => MergeStrategy.discard
//  case x                             => (assembly / assemblyMergeStrategy).value(x)
//}
