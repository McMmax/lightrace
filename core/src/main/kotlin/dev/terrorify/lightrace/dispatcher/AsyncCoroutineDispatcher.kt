package dev.terrorify.lightrace.dispatcher

import kotlinx.coroutines.CoroutineDispatcher
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext

/**
 * Async dispatcher on a dedicated daemon pool.
 *
 * Deliberately NOT the mccoroutine-minestom implementation, which scheduled
 * "async" work onto the next tick start — i.e. onto the tick thread. Blocking
 * work dispatched here never touches the tick loop.
 */
internal class AsyncCoroutineDispatcher : CoroutineDispatcher() {
    private val pool = Executors.newFixedThreadPool(
        Runtime.getRuntime().availableProcessors().coerceAtLeast(2),
        object : ThreadFactory {
            private val counter = AtomicInteger()
            override fun newThread(r: Runnable): Thread =
                Thread(r, "lightrace-async-${counter.incrementAndGet()}").apply { isDaemon = true }
        }
    )

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        pool.execute(block)
    }

    fun shutdown() {
        pool.shutdown()
    }
}
