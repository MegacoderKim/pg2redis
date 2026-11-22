plugins {
    kotlin("jvm") version "2.0.21"
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("redis.clients:jedis:5.2.0")
    implementation("org.slf4j:slf4j-nop:2.0.16")
}

application {
    mainClass.set("MainKt")
}
