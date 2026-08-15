package dev.terrorify.lightrace.internal

import dev.terrorify.lightrace.spacetime.ReducerResult
import dev.terrorify.lightrace.spacetime.SpacetimeScope
import dev.terrorify.lightrace.spacetime.TableEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.suspendCancellableCoroutine
import net.minestom.server.MinecraftServer
import net.minestom.server.timer.ExecutionType
import net.minestom.server.timer.TaskSchedule
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume

/**
 * SpacetimeDB scope backed by [SpacetimeBridge]. A single tick-start task
 * drains the inbound ring (resuming parked reducer continuations and emitting
 * table events) and flushes the outbound ring — two downcalls per tick total,
 * regardless of call/event volume.
 */
internal class SpacetimeScopeImpl(
    private val bridge: SpacetimeBridge,
    private val scope: CoroutineScope
) : SpacetimeScope, SpacetimeBridge.EventSink {

    private val nextRequestId = AtomicLong(1)
    private val pending = ConcurrentHashMap<Long, Continuation<ReducerResult>>()
    private val tableFlows = ConcurrentHashMap<Int, MutableSharedFlow<TableEvent>>()

    init {
        MinecraftServer.getSchedulerManager().buildTask { pump() }
            .repeat(TaskSchedule.tick(1))
            .executionType(ExecutionType.TICK_START)
            .schedule()
    }

    private fun pump() {
        bridge.drain(this)
        bridge.flush()
    }

    override suspend fun call(reducer: String, argsJson: String): ReducerResult {
        val id = nextRequestId.getAndIncrement()
        return suspendCancellableCoroutine { cont ->
            pending[id] = cont
            bridge.submitCall(id, reducer, argsJson)
            // Cancellation tombstones the id: a late result finds no continuation.
            cont.invokeOnCancellation { pending.remove(id) }
        }
    }

    override fun callAsync(reducer: String, argsJson: String) {
        bridge.submitCall(0, reducer, argsJson)
    }

    override fun tableEvents(tableId: Int): Flow<TableEvent> = flowFor(tableId)

    override suspend fun awaitRow(tableId: Int, predicate: (TableEvent) -> Boolean): TableEvent {
        return flowFor(tableId).filter(predicate).first()
    }

    private fun flowFor(tableId: Int): MutableSharedFlow<TableEvent> =
        tableFlows.computeIfAbsent(tableId) {
            // DROP_OLDEST keeps a slow collector from backpressuring the pump;
            // collectors needing full history should read the client cache instead.
            MutableSharedFlow(extraBufferCapacity = 1024, onBufferOverflow = BufferOverflow.DROP_OLDEST)
        }

    // --- EventSink: invoked from pump(), i.e. on the tick thread ---

    override fun onReducerResult(requestId: Long, committed: Boolean, errorMessage: String?) {
        val cont = pending.remove(requestId) ?: return
        cont.resume(if (committed) ReducerResult.Committed else ReducerResult.Failed(errorMessage ?: "unknown"))
    }

    override fun onTableEvent(event: TableEvent) {
        // Emission is on the tick thread with buffered flows — tryEmit never
        // suspends and never fails except at overflow (DROP_OLDEST).
        tableFlows[event.tableId]?.tryEmit(event)
    }

    override fun onDisconnected(cause: String?) {
        val failure = ReducerResult.Unreachable(cause?.let { RuntimeException(it) })
        val ids = pending.keys.toList()
        for (id in ids) {
            pending.remove(id)?.resume(failure)
        }
    }
}
