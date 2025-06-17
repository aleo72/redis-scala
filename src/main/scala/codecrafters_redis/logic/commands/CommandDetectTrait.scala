package codecrafters_redis.logic.commands

trait CommandDetectTrait {

  def canHandle(command: String): Boolean

}
