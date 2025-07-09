package obakalov.redis.actors.database.read

import obakalov.redis.CmdArgConfig
import obakalov.redis.actors.DatabaseActor
import obakalov.redis.actors.DatabaseActor.*
import obakalov.redis.rdb.models.*
import obakalov.redis.rdb.parser.RdbParser
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.stream.IOResult
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.util.ByteString
import obakalov.redis.rdb.RedisDataBaseStore
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.util.{Failure, Success}

trait LoadRdbFileTrait {
  val context: ActorContext[CommandOrResponse]
  val cmdArgConfig: CmdArgConfig

  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def loadRdbFile(): Future[RedisDataBaseStore] = {

    val filePath: Option[String] = for {
      realDir <- cmdArgConfig.dir
      dbFileName <- cmdArgConfig.dbfilename
    } yield s"$realDir/$dbFileName"
    val initStore: RedisDataBaseStore = createInitRedisDataBaseStore(cmdArgConfig)
    if (filePath.isEmpty) {
      Future.successful(initStore)
    } else {
      loadRdbFileFromDisk(initStore, filePath.get)
    }
  }

  private def createInitRedisDataBaseStore(cmdArgConfig: CmdArgConfig): RedisDataBaseStore = {
    new RedisDataBaseStore(cmdArgConfig)
  }

  private def loadRdbFileFromDisk(initStore: RedisDataBaseStore, filePath: String): Future[RedisDataBaseStore] = {
    given ec: scala.concurrent.ExecutionContext = context.executionContext

    given system: org.apache.pekko.actor.typed.ActorSystem[Nothing] = context.system

    val path = java.nio.file.Paths.get(filePath)
    if (!java.nio.file.Files.exists(path)) {
      context.log.warn(s"RDB file not found at path: $filePath")
      return Future.successful(initStore)
    } else {
      context.log.info(s"Loading RDB file from path: $filePath")
    }

    val source = org.apache.pekko.stream.scaladsl.FileIO.fromPath(java.nio.file.Paths.get(filePath))
    val rdbFlow: Flow[ByteString, RdbFileEntry, ?] = RdbParser.parserFlow()
    val result: Future[RedisDataBaseStore] = source
      .via(rdbFlow)
      .runFold(initStore) { (store, redisEntry) =>
        //        logger.info(s"Parsed entry: ${redisEntry}")
        redisEntry match {
          case RdbFileEntry.ResizeDb(dbNumber, dbHashTableSize, expireTimeHashTableSize) =>
            logger.info(s"Resizing database $dbNumber with hash table size $dbHashTableSize and expiry time size $expireTimeHashTableSize")
            store // No action needed for resizing in this context

          case RdbFileEntry.AuxField(key, value) =>
            logger.info(s"Auxiliary field: $key = $value")
            store // No action needed for auxiliary fields in this context

          case kv: RdbFileEntry.RedisKeyValue =>
            logger.info(s"Redis key-value pair: $kv")
            store.update(kv)
            store
        }

      }

    result.onComplete {
      case Success(database) =>
        println(s"RDB file loaded successfully with ${database.size} entries.")
      case Failure(exception) =>
        println(s"Failed to load RDB file: ${exception.getMessage}")
    }
    result
  }

}
