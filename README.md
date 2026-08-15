# ⚡ Lightrace

**Lightrace** is a lightweight context tracking and distributed tracing library for the JVM.

---

## 🎯 What does it do?
In a microservice-driven game network, a single player action (like right-clicking an NPC) might trigger logic on the proxy, an API call to a database, and an event on the game server. If something goes wrong, finding the logs for that exact sequence is like finding a needle in a haystack.

Lightrace solves this by generating a unique "trace ID" the moment an action begins, and passing that context through every single system it touches.

## 🏗️ Architecture

- **`api/`**: The clean, dependency-free interfaces and annotations used by your business logic.
- **`core/`**: The actual tracing engine, responsible for context propagation, thread-local storage, and emitting trace data.

## 🚀 Key Features
- **Thread-Safe Contexts**: Easily pass tracing context across asynchronous thread pools and coroutines without losing track of the original request.
- **Distributed Tracing**: Follow a single player's action across multiple physical servers.
- **Zero Overhead**: Designed to be incredibly lightweight so it can run continuously in production environments without impacting TPS.
