package io.flink.connector.http.architecture;

import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;

/**
 * Production code architecture rules for the Flink HTTP connector.
 */
class ProductionArchitectureRules {

    private static final String CONFIG = "io.flink.connector.http.config..";
    private static final String MODEL = "io.flink.connector.http.model..";
    private static final String CLIENT = "io.flink.connector.http.client..";
    private static final String SINK = "io.flink.connector.http.sink..";
    private static final String UTIL = "io.flink.connector.http.util..";

    @ArchTest
    static final ArchRule LAYERED_ARCHITECTURE =
            layeredArchitecture()
                    .consideringAllDependencies()
                    .layer("API").definedBy("io.flink.connector.http")
                    .layer("Config").definedBy(CONFIG)
                    .layer("Model").definedBy(MODEL)
                    .layer("Client").definedBy(CLIENT)
                    .layer("Sink").definedBy(SINK)
                    .layer("Util").definedBy(UTIL)
                    .whereLayer("Config").mayOnlyBeAccessedByLayers("API", "Client", "Sink")
                    .whereLayer("Model").mayOnlyBeAccessedByLayers("API", "Client", "Sink", "Util")
                    .whereLayer("Util").mayOnlyBeAccessedByLayers("API", "Client", "Sink", "Model")
                    .whereLayer("Client").mayOnlyBeAccessedByLayers("API", "Sink")
                    .whereLayer("Sink").mayOnlyBeAccessedByLayers("API");

    @ArchTest
    static final ArchRule CONFIG_CLASSES_SHOULD_BE_SERIALIZABLE =
            classes()
                    .that().resideInAPackage(CONFIG)
                    .and().haveSimpleNameNotEndingWith("Builder")
                    .should().implement(java.io.Serializable.class);

    @ArchTest
    static final ArchRule MODEL_CLASSES_SHOULD_BE_SERIALIZABLE =
            classes()
                    .that().resideInAPackage(MODEL)
                    .and().areNotInterfaces()
                    .and().haveSimpleNameNotEndingWith("Builder")
                    .should().implement(java.io.Serializable.class);

    @ArchTest
    static final ArchRule MODEL_SHOULD_NOT_DEPEND_ON_CLIENT_IMPL =
            noClasses()
                    .that().resideInAPackage(MODEL)
                    .should().dependOnClassesThat().resideInAPackage("io.flink.connector.http.client.apache..");

    @ArchTest
    static final ArchRule UTIL_SHOULD_NOT_DEPEND_ON_IMPL =
            noClasses()
                    .that().resideInAPackage(UTIL)
                    .should().dependOnClassesThat().resideInAPackage("io.flink.connector.http.client.apache..");

    @ArchTest
    static final ArchRule NO_CYCLES =
            com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices()
                    .matching("io.flink.connector.http.(*)..")
                    .should().beFreeOfCycles();
}
