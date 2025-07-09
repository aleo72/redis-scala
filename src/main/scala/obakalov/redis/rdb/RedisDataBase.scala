package obakalov.redis.rdb

import obakalov.redis.CmdArgConfig
import obakalov.redis.rdb.models.RdbFileEntry
import obakalov.redis.rdb.models.RdbValue
import obakalov.redis.rdb.models.RdbHeader

import scala.collection.concurrent.TrieMap

class RedisDataBaseStore(private val cmdConfig: CmdArgConfig) {

  private val metadata: RdbHeader = RdbHeader(0, 0, 0, 0, Map.empty)
  private val databases: Seq[RedisDataBase] = Seq.fill(cmdConfig.countDatabases)(new RedisDataBase())

  private def checkDatabaseIndex(db: Int): Unit =
    if (db < 0 || db >= databases.length) throw new IllegalArgumentException(s"Database index $db is out of bounds")

  def keys(db: Int): Set[String] = {
    checkDatabaseIndex(db)
    databases(db).keys
  }
  
  def get(db: Int, key: String): Option[RdbValue] = {
    checkDatabaseIndex(db)
    databases(db).get(key)
  }

  def update(kv: RdbFileEntry.RedisKeyValue): Unit = update(kv.dbNumber, kv.key, kv.value)

  def update(db: Int, key: String, value: RdbValue): Unit = {
    checkDatabaseIndex(db)
    databases(db).update(key, value)
  }

  def size: Int = databases.foldLeft(0)((acc, db) => acc + db.size)

}

class RedisDataBase {

  private val store: TrieMap[String, RdbValue] = TrieMap.empty[String, RdbValue]

  private def isNotExpired(value: RdbValue): Boolean = value.expireAt.forall(_ > System.currentTimeMillis())
  private def isNotExpired(kv: (String, RdbValue)): Boolean = isNotExpired(kv._2)
  
  def get(key: String): Option[RdbValue] = store.get(key).filter(isNotExpired)
  def update(key: String, value: RdbValue): Unit = store.update(key, value)
  def size: Int = store.size
  def keys: Set[String] = store.filter(isNotExpired).keys.toSet

}
