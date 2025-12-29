# Project Description for AI Assistant

## Project Overview

This project is an implementation of a Redis server in Scala. It is part of the [CodeCrafters.io](https://codecrafters.io/) "Build Your Own Redis" challenge.

The goal of this project is to build a Redis-compatible server from scratch, handling TCP connections, parsing the Redis protocol (RESP), and implementing various Redis commands.

## Working with Scala

When assisting with this project, please adhere to the following guidelines for writing Scala code:

*   **Style**: Follow the official [Scala Style Guide](https://docs.scala-lang.org/style/).
*   **Idiomatic Scala**: Prefer functional programming constructs over imperative ones. Use immutable data structures, `case class`es, `Option`, `Either`, and `Future` where appropriate.
*   **Actors**: The project uses the Akka actor model for concurrency. Be familiar with actor creation, message passing (`!`), and actor state management.
*   **Pattern Matching**: Use pattern matching extensively for message handling in actors and for destructuring data.
*   **Dependencies**: The project uses `sbt` for dependency management. Any new dependencies should be added to the `build.sbt` file.

## Key Project Components

*   **Server**: The main server entry point that listens for client connections.
*   **Actors**:
    *   `ClientActor`: Manages the lifecycle of a single client connection.
    *   `DatabaseActor`: Manages the state of the Redis database (key-value store).
    *   Command-specific logic handlers.
*   **RESP Protocol**: Code for parsing and serializing the Redis Serialization Protocol.
*   **Commands**: Implementation of various Redis commands like `PING`, `ECHO`, `SET`, `GET`, `CONFIG`, `KEYS`.

When adding new commands or modifying existing ones, ensure that the implementation correctly handles the RESP protocol and interacts with the `DatabaseActor` for state changes.
