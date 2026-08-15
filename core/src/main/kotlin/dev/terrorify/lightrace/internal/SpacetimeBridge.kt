package dev.terrorify.lightrace.internal

import dev.terrorify.lightrace.spacetime.TableEvent

/**
 * SPI over the Rust FFI bridge (minestom_stdb_bridge). The Stomzy spacetime
 * module provides the implementation backed by the shared-memory event ring;
 * lightrace-core only sees this interface, so it stays engine-agnostic and
 * unit-testable with an in-memory fake.
 */
interface SpacetimeBridge {
    /**
     * Drains every event accumulated since the last drain — ONE downcall,
     * invoked once per tick from the pump on the tick thread. Payload byte
     * arrays are only valid during the callback (they view ring memory).
     */
    fun drain(sink: EventSink)

    /**
     * Appends a reducer call to the outbound ring. [requestId] correlates the
     * eventual result event; pass 0 for fire-and-forget (no result event
     * requested).
     */
    fun submitCall(requestId: Long, reducer: String, argsJson: String)

    /** Flushes the outbound ring to the SDK connection (one downcall). */
    fun flush()

    interface EventSink {
        fun onReducerResult(requestId: Long, committed: Boolean, errorMessage: String?)
        fun onTableEvent(event: TableEvent)
        fun onDisconnected(cause: String?)
    }
}
