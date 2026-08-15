package dev.terrorify.lightrace.impl

import dev.terrorify.lightrace.CoroutineSession
import dev.terrorify.lightrace.LightRace
import dev.terrorify.lightrace.internal.SpacetimeBridge
import dev.terrorify.lightrace.spacetime.SpacetimeScope
import dev.terrorify.lightrace.spacetime.TableEvent
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import net.minestom.server.MinecraftServer
import java.util.function.Consumer

/**
 * Default driver, instantiated reflectively by the api module.
 * One session per JVM — Stomzy runs one MinecraftServer per process.
 */
class LightRaceImpl : LightRace {
    companion object {
        /**
         * Set by the Stomzy spacetime module during bootstrap, BEFORE the first
         * coroutine touches `server.spacetime`. Left null, spacetime access
         * throws with a clear message while the dispatcher layer still works.
         */
        @JvmStatic
        @Volatile
        var bridge: SpacetimeBridge? = null

        /**
         * The single JVM-lifetime instance, captured on construction so plain
         * Java callers (no `MinecraftServer` receiver in scope, e.g. static
         * bootstrap code) can reach [SpacetimeScope] without going through the
         * `MinecraftServer.spacetime` Kotlin extension property. `getSession`
         * ignores its `server` argument (one session per JVM), so this is
         * equivalent to `LightRace.getSession(anyServer).spacetime`.
         */
        @Volatile
        private var instance: LightRaceImpl? = null

        /** Java-facing accessor for the process-wide [SpacetimeScope]. */
        @JvmStatic
        fun spacetimeScope(): SpacetimeScope =
            (instance ?: error("LightRaceImpl not yet constructed")).session.spacetime

        /**
         * Java-friendly bridge onto [SpacetimeScope.tableEvents]: subscribes [onEvent]
         * to every row event for [tableId] for the lifetime of the JVM session.
         * `awaitRow`/`tableEvents` are suspend APIs / Kotlin `Flow`s that plain Java
         * Java callers can't invoke directly — this is the minimal
         * callback-style wrapper for that case, mirroring the fire-and-forget style
         * of [SpacetimeScope.callAsync]. Returns `void` rather than the underlying
         * coroutine `Job` so this method — and its return type — stay resolvable
         * from a plain `compileJava` classpath that doesn't depend on
         * kotlinx-coroutines directly; there is currently no unsubscribe, callers
         * are expected to install once at bootstrap (mirrors
         * consumers' idempotent scheduler setup).
         *
         * Exceptions from [onEvent] are routed to the session's exception handler
         * (same place uncaught coroutine exceptions in mccoroutine-style code land)
         * rather than killing the collector.
         */
        @JvmStatic
        fun collectTableEvents(tableId: Int, onEvent: Consumer<TableEvent>) {
            val impl = instance ?: error("LightRaceImpl not yet constructed")
            impl.session.scope.launch {
                impl.session.spacetime.tableEvents(tableId).collect { event -> onEvent.accept(event) }
            }
        }
    }

    init {
        instance = this
    }

    private val session by lazy {
        CoroutineSessionImpl {
            bridge ?: error(
                "LightRace SpacetimeBridge not installed. " +
                    "SpacetimeBootstrap must set LightRaceImpl.bridge before spacetime access."
            )
        }
    }

    override fun getSession(server: MinecraftServer): CoroutineSession = session

    override fun disable(server: MinecraftServer) {
        session.dispose()
    }
}
