// lightrace-api — Kotlin coroutines + SpacetimeDB integration API for Stomzy.
//
// Terrorify's successor to mccoroutine-minestom: same dispatcher/launch surface,
// plus a SpacetimeDB layer (suspend reducer calls, table Flows, awaitRow) backed
// by the Rust FFI bridge (minestom_stdb_bridge) instead of a traditional database.
//
// Consumed as a Stomzy subproject (see platforms/stomzy/settings.gradle.kts,
// :lightrace-api) — same projectDir pattern as :mccoroutine-minestom-api.

plugins {
    // Matches mccoroutine-minestom's kotlin("jvm") version — Kotlin module metadata
    // isn't binary-compatible across compiler versions and consumers compile against
    // this module's Kotlin declarations.
    kotlin("jvm") version "2.4.0"
    `java-library`
}

group = "dev.terrorify.lightrace"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    // The Stomzy engine core (this repo IS a Minestom fork — net.minestom.server
    // is compiled at the composite-build root, not an external artifact).
    compileOnly(project(":"))
    compileOnly("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
}

// See libraries/mccoroutine-minestom/api/build.gradle.kts for the toolchain rationale
// (engine core requires JVM 26; Kotlin 2.4.0 targets Java 26 bytecode directly).
kotlin {
    jvmToolchain(26)
}

// Stable JPMS automatic-module name so named modules (net.minestom.server.spacetime)
// can `requires dev.terrorify.lightrace.api` instead of a filename-derived name.
tasks.jar {
    manifest.attributes["Automatic-Module-Name"] = "dev.terrorify.lightrace.api"
}
