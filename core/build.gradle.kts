// lightrace-core — implementation of the lightrace API for Stomzy.
//
// Hosts the tick-thread/async dispatchers, the coroutine session, and the
// SpacetimeDB pump: a once-per-tick drain of the Rust bridge's shared-memory
// event ring that resumes parked continuations and emits table Flows.
//
// Consumed as a Stomzy subproject (see platforms/stomzy/settings.gradle.kts,
// :lightrace-core).

plugins {
    kotlin("jvm") version "2.4.0"
    `java-library`
}

group = "dev.terrorify.lightrace"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    api(project(":lightrace-api"))

    // The Stomzy engine core (this repo IS a Minestom fork).
    implementation(rootProject)
    compileOnly("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
}

// See ../api/build.gradle.kts.
kotlin {
    jvmToolchain(26)
}

// See ../api/build.gradle.kts.
tasks.jar {
    manifest.attributes["Automatic-Module-Name"] = "dev.terrorify.lightrace.core"
}
