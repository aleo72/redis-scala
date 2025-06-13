package codecrafters_redis

import akka.actor.typed.ActorSystem
import codecrafters_redis.actors.ServerActor

object Server {
  def main(args: Array[String]): Unit = {
    println("Logs from your program will appear here!")

    val system = ActorSystem(ServerActor.props(6379), "RedisServerSystem")

  }

}
