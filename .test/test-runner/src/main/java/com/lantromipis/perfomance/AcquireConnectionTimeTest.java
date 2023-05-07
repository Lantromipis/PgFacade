package com.lantromipis.perfomance;

import com.lantromipis.PerformanceTestsProperties;
import com.lantromipis.utils.MathUtils;
import com.lantromipis.utils.PostgresUtils;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@ApplicationScoped
public class AcquireConnectionTimeTest {

    @Inject
    PerformanceTestsProperties performanceTestsProperties;

    public void runTest() {
        try {
            log.info("Running aquire connection test for original Postgres on host {} and port {}. Will connect to Postgres {} times an measure mean acquire connection time.",
                    performanceTestsProperties.originalPostgresHost().host(),
                    performanceTestsProperties.originalPostgresHost().port(),
                    performanceTestsProperties.testAquireConnectionTime().retries());

            List<Long> originalPostgres = runTest(performanceTestsProperties.originalPostgresHost());

            log.info("Running aquire connection test for PgFacade on host {} and port {}. Will connect to Postgres {} times an measure mean acquire connection time.",
                    performanceTestsProperties.pgFacadeHost().host(),
                    performanceTestsProperties.pgFacadeHost().port(),
                    performanceTestsProperties.testAquireConnectionTime().retries());

            List<Long> pgFacade = runTest(performanceTestsProperties.pgFacadeHost());

            List<Long> pgBouncer = null;
            if (performanceTestsProperties.runPgBouncer()) {
                log.info("Running aquire connection test for PgBouncer on host {} and port {}. Will connect to Postgres {} times an measure mean acquire connection time.",
                        performanceTestsProperties.pgBouncerHost().host(),
                        performanceTestsProperties.pgBouncerHost().port(),
                        performanceTestsProperties.testAquireConnectionTime().retries());
                pgBouncer = runTest(performanceTestsProperties.pgBouncerHost());
            }

            log.info("Math expected time to acquire connection using original Postgres after {} probes: {} ms. Min {} ms, Max {} ms",
                    performanceTestsProperties.testAquireConnectionTime().retries(),
                    MathUtils.calculateExpectedValue(originalPostgres),
                    MathUtils.calculateMinValue(originalPostgres),
                    MathUtils.calculateMaxValue(originalPostgres)
            );

            log.info("Math expected time to acquire connection using PgFacade after {} probes: {} ms. Min {} ms, Max {} ms",
                    performanceTestsProperties.testAquireConnectionTime().retries(),
                    MathUtils.calculateExpectedValue(pgFacade),
                    MathUtils.calculateMinValue(pgFacade),
                    MathUtils.calculateMaxValue(pgFacade)
            );

            if (performanceTestsProperties.runPgBouncer() && pgBouncer != null) {
                log.info("Math expected time to acquire connection using PgBouncer after {} probes: {} ms. Min {} ms, Max {} ms",
                        performanceTestsProperties.testAquireConnectionTime().retries(),
                        MathUtils.calculateExpectedValue(pgBouncer),
                        MathUtils.calculateMinValue(pgBouncer),
                        MathUtils.calculateMaxValue(pgBouncer)
                );
            }

        } catch (Exception e) {
            log.error("Unable to run aquire connection time tests! ", e);
        }
    }

    private List<Long> runTest(PerformanceTestsProperties.HostProperties hostProperties) throws Exception {
        String jdbcUrl = PostgresUtils.createJdbcUrl(
                hostProperties.host(),
                hostProperties.port(),
                performanceTestsProperties.pgUser().database()
        );

        String username = performanceTestsProperties.pgUser().user();
        String password = performanceTestsProperties.pgUser().password();


        // get connection to fill pool with 1 connection
        Connection connection = DriverManager.getConnection(
                jdbcUrl,
                username,
                password
        );
        connection.close();

        List<Long> results = new ArrayList<>();

        int logProgressStep = performanceTestsProperties.testAquireConnectionTime().retries() / 10;
        int currentStep = 1;

        for (int i = 0; i < performanceTestsProperties.testAquireConnectionTime().retries(); i++) {
            long start = System.currentTimeMillis();
            connection = DriverManager.getConnection(
                    jdbcUrl,
                    username,
                    password
            );
            long end = System.currentTimeMillis();
            connection.close();
            results.add(end - start);
            Thread.sleep(performanceTestsProperties.testAquireConnectionTime().waitBetween());

            if (i == logProgressStep * currentStep) {
                log.info("Progress report: {} out of {}", i, performanceTestsProperties.testAquireConnectionTime().retries());
                currentStep++;
            }
        }

        return results;
    }
}
