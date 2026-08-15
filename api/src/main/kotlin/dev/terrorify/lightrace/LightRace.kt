package dev.terrorify.lightrace

import dev.terrorify.lightrace.spacetime.SpacetimeScope
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import net.minestom.server.MinecraftServer
import net.minestom.server.command.CommandSender
import net.minestom.server.command.builder.Command
import net.minestom.server.command.builder.CommandContext
import net.minestom.server.event.Event
import net.minestom.server.event.EventListener
import net.minestom.server.event.EventNode
import kotlin.coroutines.CoroutineContext

/**
 * Static session, loaded from the core module via the driver class name
 * (shade lightrace-core into the application).
 */
internal val lightRace: LightRace by lazy {
    try {
        Class.forName(LightRace.Driver)
            .getDeclaredConstructor().newInstance() as LightRace
    } catch (e: Exception) {
        throw RuntimeException(
            "Failed to load LightRace implementation. Shade lightrace-core into your application.",
            e
        )
    }
}

/** Gets the lightrace configuration. */
val MinecraftServer.lightRaceConfiguration: LightRaceConfiguration
    get() = lightRace.getSession(this).configuration

/** Gets the tick-thread dispatcher. */
val MinecraftServer.minecraftDispatcher: CoroutineContext
    get() = lightRace.getSession(this).dispatcherMinecraft

/** Gets the async dispatcher. */
val MinecraftServer.asyncDispatcher: CoroutineContext
    get() = lightRace.getSession(this).dispatcherAsync

/** Gets the server-lifetime coroutine scope. */
val MinecraftServer.scope: CoroutineScope
    get() = lightRace.getSession(this).scope

/** Gets the SpacetimeDB scope. */
val MinecraftServer.spacetime: SpacetimeScope
    get() = lightRace.getSession(this).spacetime

/**
 * Launches a coroutine in the server scope. Defaults to [minecraftDispatcher]:
 * runs inline immediately when already on the tick thread, otherwise starts at
 * the next tick. Uncaught exceptions fire [LightRaceExceptionEvent].
 */
fun MinecraftServer.launch(
    context: CoroutineContext = minecraftDispatcher,
    start: CoroutineStart = CoroutineStart.DEFAULT,
    block: suspend CoroutineScope.() -> Unit
): Job {
    if (!scope.isActive) {
        return Job()
    }
    return scope.launch(context, start, block)
}

/**
 * Converts ticks to milliseconds for delay(). Offset by -25ms so that
 * delay(1.ticks) lands inside the next tick rather than skipping one
 * (same behavior as mccoroutine).
 */
val Int.ticks: Long
    get() = this * 50L - 25

/** Sets a suspending default executor on a command. */
fun Command.setSuspendingDefaultExecutor(
    server: MinecraftServer,
    executor: suspend (CommandSender, CommandContext) -> Unit
) {
    this.setDefaultExecutor { sender, context ->
        server.launch {
            executor.invoke(sender, context)
        }
    }
}

/** Adds a suspending listener to this event node. */
fun <E : Event> EventNode<in E>.addSuspendingListener(
    server: MinecraftServer,
    eventType: Class<E>,
    listener: suspend (E) -> Unit
) {
    this.addListener(eventType) { e ->
        server.launch {
            listener.invoke(e)
        }
    }
}

/** Adds a suspending handler to this listener builder. */
fun <E : Event> EventListener.Builder<E>.suspendingHandler(
    server: MinecraftServer,
    listener: suspend (E) -> Unit
): EventListener.Builder<E> {
    return this.handler { e ->
        server.launch {
            listener.invoke(e)
        }
    }
}

/**
 * Hidden internal LightRace driver interface.
 */
interface LightRace {
    companion object {
        /** Driver class name; swappable for unit tests. */
        var Driver: String = "dev.terrorify.lightrace.impl.LightRaceImpl"
    }

    /** Gets the coroutine session bound to the server lifetime. */
    fun getSession(server: MinecraftServer): CoroutineSession

    /** Disposes the session. */
    fun disable(server: MinecraftServer)
}
