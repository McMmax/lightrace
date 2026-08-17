package dev.terrorify.lightrace.dispatcher

import kotlinx.coroutines.CoroutineDispatcher
import net.minestom.server.MinecraftServer
import net.minestom.server.timer.ExecutionType
import kotlin.coroutines.CoroutineContext

/**
 * Tick-thread dispatcher. Runs inline when already on the tick thread —
 * avoiding a full-tick (50ms) hop per resume — otherwise schedules at the
 * next tick start.
 */
internal class MinecraftCoroutineDispatcher : CoroutineDispatcher() {
    @Volatile
    private var tickThreadId: Long = -1

    init {
        MinecraftServer.getSchedulerManager().scheduleNextProcess {
            tickThreadId = Thread.currentThread().threadId()
        }
    }

    override fun isDispatchNeeded(context: CoroutineContext): Boolean {
        return Thread.currentThread().threadId() != tickThreadId
    }

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        MinecraftServer.getSchedulerManager().scheduleNextTick(block, ExecutionType.TICK_START)
    }
}
