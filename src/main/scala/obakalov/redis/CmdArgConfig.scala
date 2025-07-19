package obakalov.redis

import ch.qos.logback.classic.Logger

case class CmdArgConfig(
    port: Int = 6379,
    dir: Option[String] = None,
    dbfilename: Option[String] = None,
    countDatabases: Int = 16,
    replicaof: Option[String] = None
)

object CmdArgConfigParser {

  def parse(args: Array[String]): CmdArgConfig = {
    val parser = new scopt.OptionParser[CmdArgConfig]("codecrafters-redis") {
      head("codecrafters-redis", "0.1.0")

      opt[Int]('p', "port")
        .action((p, c) => c.copy(port = p))
        .text("Port to run the Redis server on (default: 6379)")
        .validate(p => if (p > 0 && p <= 65535) success else failure("Port must be between 1 and 65535"))

      opt[String]("dir")
        .action((d, c) => c.copy(dir = Some(d)))
        .text("Directory to store the database files (default: current directory)")
        .validate(d => if (d.nonEmpty) success else failure("Directory cannot be empty"))

      opt[String]("dbfilename")
        .action((f, c) => c.copy(dbfilename = Some(f)))
        .text("Name of the database file (default: dump.rdb)")
        .validate(f => if (f.nonEmpty) success else failure("Database filename cannot be empty"))

      opt[Int]("countDatabases")
        .action((c, cfg) => cfg.copy(countDatabases = c))
        .text("Number of databases to create (default: 16)")
        .validate(c =>
          if (c > 0) success
          else failure("Number of databases must be greater than 0")
        )

      opt[String]("replicaof")
        .action((r, c) => c.copy(replicaof = Some(r).filter(_.nonEmpty)))
        .text("Replica of another Redis server (format: host port)")
        .validate(r =>
          if (r.matches("^[^:]+ \\d+$")) success
          else failure("Replicaof must be in the format \"host port\"")
        )

    }

    val value = parser.parse(args, CmdArgConfig()).getOrElse {
      throw new IllegalArgumentException(s"Invalid command line arguments: ${args.mkString("[", ",", "]")}")
    }
    println(s"Parsed command line arguments: $value")
    value
  }
}
