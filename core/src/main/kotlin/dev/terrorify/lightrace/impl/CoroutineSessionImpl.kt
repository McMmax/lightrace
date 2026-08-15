package dev.terrorify.lightrace.impl

import dev.terrorify.lightrace.CoroutineSession
import dev.terrorify.lightrace.LightRaceConfiguration
import dev.terrorify.lightrace.LightRaceExceptionEvent
import dev.terrorify.lightrace.ShutdownStrategy
import dev.terrorify.lightrace.dispatcher.AsyncCoroutineDispatcher
import dev.terrorify.lightrace.dispatcher.MinecraftCoroutineDispatcher
import dev.terrorify.lightrace.internal.SpacetimeBridge
import dev.terrorify.lightrace.spacetime.SpacetimeScope
import dev.terrorify.lightrace.internal.SpacetimeScopeImpl
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.plus
import net.minestom.server.MinecraftServer
import net.minestom.server.event.EventDispatcher
import kotlin.coroutines.CoroutineContext

internal class CoroutineSessionImpl(
    bridgeProvider: () -> SpacetimeBridge
) : CoroutineSession {

    override val configuration: LightRaceConfiguration = object : LightRaceConfiguration {
        override var shutdownStrategy: ShutdownStrategy = ShutdownStrategy.GRACEFUL
        override var shutdownTimeoutMillis: Long = 5000
        override fun disposeSession() = dispose()
    }

    override val dispatcherMinecraft: CoroutineContext by lazy { MinecraftCoroutineDispatcher() }

    private val asyncDispatcherImpl by lazy { AsyncCoroutineDispatcher() }
    override val dispatcherAsync: CoroutineContext get() = asyncDispatcherImpl

    override val scope: CoroutineScope

    override val spacetime: SpacetimeScope by lazy {
        SpacetimeScopeImpl(bridgeProvider(), scope)
    }

    init {
        // Root handler: uncaught coroutine exceptions surface as a cancellable
        // event on the tick thread, mirroring mccoroutine's contract.
        val exceptionHandler = CoroutineExceptionHandler { _, e ->
            MinecraftServer.getSchedulerManager().scheduleNextTick {
                val event = LightRaceExceptionEvent(e)
                EventDispatcher.call(event)
                if (!event.isCancelled) {
                    MinecraftServer.LOGGER.error(
                        "Uncaught exception in a lightrace coroutine. See sub exception for details.", e
                    )
                }
            }
        }
        scope = CoroutineScope(exceptionHandler) + SupervisorJob() + dispatcherMinecraft
    }

    fun dispose() {
        scope.coroutineContext.cancelChildren()
        scope.cancel()
        asyncDispatcherImpl.shutdown()
    }
}
