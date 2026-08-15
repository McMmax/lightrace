package dev.terrorify.lightrace

/**
 * What happens to still-running coroutines when the server shuts down.
 */
enum class ShutdownStrategy {
    /** Cancel all pending coroutines immediately. */
    IMMEDIATE,

    /** Wait for pending coroutines to finish (bounded by [LightRaceConfiguration.shutdownTimeoutMillis]). */
    GRACEFUL
}

/**
 * Session configuration.
 */
interface LightRaceConfiguration {
    var shutdownStrategy: ShutdownStrategy
    var shutdownTimeoutMillis: Long

    /**
     * Disposes the session (cancels or drains per [shutdownStrategy]).
     */
    fun disposeSession()
}
