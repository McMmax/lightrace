package dev.terrorify.lightrace

import net.minestom.server.event.trait.CancellableEvent

/**
 * Fired on the tick thread when a lightrace coroutine dies with an uncaught
 * exception. Cancel the event to suppress the default error log.
 */
class LightRaceExceptionEvent(val throwable: Throwable) : CancellableEvent {
    private var cancelled = false
    override fun isCancelled(): Boolean = cancelled
    override fun setCancelled(cancel: Boolean) {
        cancelled = cancel
    }
}
