plugins {
    kotlin("jvm") version "2.1.20"
}

group = "com.sz.file_service.lib"
version = "0.1"

repositories {
    mavenCentral()
}

dependencies {
    api("org.apache.commons:commons-compress:1.27.1")
    implementation(files("../../smart_home/smart_home_common/build/libs/smart_home_common-0.1.jar"))
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    from(sourceSets.main.get().output)
}
