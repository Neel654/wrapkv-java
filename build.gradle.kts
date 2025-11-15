plugins {
    application
    java
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.netty:netty-all:4.1.111.Final")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.3")
}

application {
    mainClass = "com.neel.warpkv.server.Server"
}

tasks.test {
    useJUnitPlatform()
}

/**
 * Build an "uber" JAR without the Shadow plugin.
 * Usage: ./gradlew clean uberJar
 * Output: build/libs/warpkv-java-uber.jar
 */
tasks.register<Jar>("uberJar") {
    group = "build"
    description = "Build a self-contained JAR with all runtime deps"

    archiveBaseName.set("warpkv-java")
    archiveClassifier.set("uber")

    // Include compiled classes from this project
    from(sourceSets.main.get().output)

    // Unpack all runtime JARs into this JAR
    val runtimeClasspath = configurations.runtimeClasspath.get()
    from(runtimeClasspath.filter { it.name.endsWith(".jar") }.map { zipTree(it) })

    // Avoid duplicate files clashing during merge
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    // Set the Main-Class so 'java -jar' works
    manifest {
        attributes["Main-Class"] = application.mainClass.get()
    }
}

