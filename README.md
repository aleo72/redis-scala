[![progress-banner](https://backend.codecrafters.io/progress/redis/16f3a31c-5271-43f6-8dfd-53aee828b146)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is a starting point for Scala solutions to the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

In this challenge, you'll build a toy Redis clone that's capable of handling
basic commands like `PING`, `SET` and `GET`. Along the way we'll learn about
event loops, the Redis protocol and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Passing the first stage

The entry point for your Redis implementation is in
`src/main/scala/codecrafters_redis/Server.scala`. Study and uncomment the
relevant code, and push your changes to pass the first stage:

```sh
git commit -am "pass 1st stage" # any msg
git push origin master
```

That's all!

# Stage 2 & beyond

Note: This section is for stages 2 and beyond.

1. Ensure you have `sbt (1.9.9)` installed locally
1. Run `./your_program.sh` to run your Redis server, which is implemented in
   `src/main/scala/codecrafters_redis/Server.scala`.
1. Commit your changes and run `git push origin master` to submit your solution
   to CodeCrafters. Test output will be streamed to your terminal.
