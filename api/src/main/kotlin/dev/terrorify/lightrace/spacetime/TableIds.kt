package dev.terrorify.lightrace.spacetime

/**
 * Well-known `table_id` values used with [SpacetimeScope.tableEvents] /
 * [SpacetimeScope.awaitRow].
 *
 * These MUST mirror the `table_ids` module in the Rust native bridge
 * (`platforms/stomzy/spacetime/native/src/lightrace.rs`). There is no codegen
 * step tying the two together yet, so any addition/change here needs a
 * matching change on the Rust side (and vice versa) or table events will
 * silently land on the wrong id.
 */
object TableIds {
    /** Mirrors `table_ids::QUEUE_CLEARANCE` in lightrace.rs. */
    const val QUEUE_CLEARANCE: Int = 1

    /** Mirrors `table_ids::PRISON_PLAYER` in lightrace.rs. */
    const val PRISON_PLAYER: Int = 2

    /** Mirrors `table_ids::PRISON_PROFILE_RESULT` in lightrace.rs. */
    const val PRISON_PROFILE_RESULT: Int = 3
}
