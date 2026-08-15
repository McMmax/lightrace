package dev.terrorify.lightrace

import dev.terrorify.lightrace.spacetime.SpacetimeScope
import kotlinx.coroutines.CoroutineScope
import kotlin.coroutines.CoroutineContext

/**
 * Facade of a coroutine session bound to the server lifetime.
 */
interface CoroutineSession {
    /**
     * Lifetime scope. Cancelled when the server shuts down.
     */
    val scope: CoroutineScope

    /**
     * Tick-thread dispatcher. Runs inline when already on the tick thread
     * (no 1-tick hop), otherwise schedules for the next tick start.
     */
    val dispatcherMinecraft: CoroutineContext

    /**
     * Async dispatcher for blocking or CPU-bound work off the tick thread.
     */
    val dispatcherAsync: CoroutineContext

    /**
     * SpacetimeDB access bound to this session's lifetime.
     */
    val spacetime: SpacetimeScope

    /**
     * Configuration facade.
     */
    val configuration: LightRaceConfiguration
}
