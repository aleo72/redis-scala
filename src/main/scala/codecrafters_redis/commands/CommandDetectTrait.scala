package codecrafters_redis.commands

trait CommandDetectTrait {

  def commandName: String
  
  def canHandle(command: ProtocolMessage): Boolean

}
