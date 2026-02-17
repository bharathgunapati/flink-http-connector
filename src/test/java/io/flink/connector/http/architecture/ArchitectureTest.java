package io.flink.connector.http.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

/**
 * Architecture tests for the Flink HTTP connector, following Flink's architecture test patterns.
 *
 * @see <a href="https://www.archunit.org/">ArchUnit</a>
 */
@AnalyzeClasses(
        packages = "io.flink.connector.http",
        importOptions = {ImportOption.DoNotIncludeTests.class})
class ArchitectureTest {

    @ArchTest
    static final ArchTests PRODUCTION_RULES = ArchTests.in(ProductionArchitectureRules.class);
}
