plugins {
    kotlin("jvm") version "1.8.0"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.elasticsearch.client:elasticsearch-rest-client:8.8.1")
    implementation ("co.elastic.clients:elasticsearch-java:8.8.1")
    implementation ("com.fasterxml.jackson.core:jackson-databind:2.12.3")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set("MainKt")
}