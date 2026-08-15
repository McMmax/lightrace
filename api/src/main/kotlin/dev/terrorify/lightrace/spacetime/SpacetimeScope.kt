package dev.terrorify.lightrace.spacetime

import kotlinx.coroutines.flow.Flow

/**
 * Outcome of an awaited reducer call, correlated by request id through the
 * bridge's event ring.
 */
sealed interface ReducerResult {
    /** The reducer committed. */
    data object Committed : ReducerResult

    /** The reducer failed server-side; [message] is the error the module raised. */
    data class Failed(val message: String) : ReducerResult

    /** The call never reached the database (disconnected, bridge down). */
    data class Unreachable(val cause: Throwable?) : ReducerResult
}

/**
 * A single row event drained from the bridge's event ring. [payload] is the
 * BSATN-encoded row still in bridge memory at drain time — decode it during
 * the tick it was delivered; do not retain the reference across ticks.
 */
class TableEvent(
    val tableId: Int,
    val kind: Kind,
    val payload: ByteArray
) {
    enum class Kind { INSERT, UPDATE, DELETE }
}

/**
 * SpacetimeDB access for lightrace coroutines. All suspend functions resume on
 * the caller's dispatcher; Flows emit on the tick thread, batched per tick.
 */
interface SpacetimeScope {
    /**
     * Calls a reducer and suspends until the result event comes back through
     * the ring. Cancellation tombstones the request id — a late result is dropped.
     */
    suspend fun call(reducer: String, argsJson: String): ReducerResult

    /**
     * Fire-and-forget reducer call: appended to the outbound ring, flushed at
     * the end of the current tick (or immediately if off the tick thread).
     */
    fun callAsync(reducer: String, argsJson: String)

    /**
     * Raw per-row events for a table, emitted on the tick thread. Collection
     * lifetime is the collector's coroutine scope — cancelling the scope
     * unsubscribes.
     */
    fun tableEvents(tableId: Int): Flow<TableEvent>

    /**
     * Suspends until a row event for [tableId] matches [predicate], returning
     * that event. Combine with a timeout at the call site.
     */
    suspend fun awaitRow(tableId: Int, predicate: (TableEvent) -> Boolean): TableEvent
}
