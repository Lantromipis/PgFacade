package com.lantromipis.perfomance;

import com.lantromipis.PerformanceTestsProperties;
import com.lantromipis.utils.MathUtils;
import com.lantromipis.utils.PostgresUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@ApplicationScoped
public class ProxyEffectOnDelayTest {

    @Inject
    PerformanceTestsProperties performanceTestsProperties;

    public void runTest() {
        try {
            log.info("Starting delay test for original Postgres...");
            List<Long> pgTest = runTest(performanceTestsProperties.originalPostgresHost());

            log.info("Starting delay test for PgFacade proxy...");
            List<Long> pgFacadeTest = runTest(performanceTestsProperties.pgFacadeHost());

            List<Long> pgBouncerTest = null;
            if (performanceTestsProperties.runPgBouncer()) {
                log.info("Starting delay test for PgBouncer...");
                pgBouncerTest = runTest(performanceTestsProperties.pgBouncerHost());
            }

            log.info("Delay test results for original Postgres after {} SELECT retries for table of size {} kb : math expected value {}, min {}, max {} ms",
                    performanceTestsProperties.delayTest().retries(),
                    performanceTestsProperties.delayTest().tableSizeKb(),
                    MathUtils.calculateExpectedValue(pgTest),
                    MathUtils.calculateMinValue(pgTest),
                    MathUtils.calculateMaxValue(pgTest)
            );

            log.info("Delay test results for PgFacade proxy after {} SELECT retries for table of size {} kb : math expected value {} ms, min {} ms, max {} ms",
                    performanceTestsProperties.delayTest().retries(),
                    performanceTestsProperties.delayTest().tableSizeKb(),
                    MathUtils.calculateExpectedValue(pgFacadeTest),
                    MathUtils.calculateMinValue(pgFacadeTest),
                    MathUtils.calculateMaxValue(pgFacadeTest)
            );

            if (performanceTestsProperties.runPgBouncer() && pgBouncerTest != null) {
                log.info("Delay test results for PgBouncer after {} SELECT retries for table of size {} kb : math expected value {} ms, min {} ms, max {} ms",
                        performanceTestsProperties.delayTest().retries(),
                        performanceTestsProperties.delayTest().tableSizeKb(),
                        MathUtils.calculateExpectedValue(pgBouncerTest),
                        MathUtils.calculateMinValue(pgBouncerTest),
                        MathUtils.calculateMaxValue(pgBouncerTest)
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

        Connection connection = DriverManager.getConnection(
                jdbcUrl,
                username,
                password
        );

        List<Long> ret = new ArrayList<>();

        connection.createStatement().executeUpdate("DROP TABLE IF EXISTS test");
        connection.createStatement().executeUpdate("CREATE TABLE test(id bigserial, data varchar(32))");
        log.info("Preparing table for delay test...");

        int steps = performanceTestsProperties.delayTest().tableSizeKb() * 1024 / 32;

        int logProgressStep = steps / 10;
        int currentStep = 1;

        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO test (data) VALUES(?)");

        for (int i = 0; i < steps; i++) {
            String randomString = RandomStringUtils.random(32, true, true);
            preparedStatement.setString(1, randomString);
            preparedStatement.executeUpdate();

            if (i == logProgressStep * currentStep) {
                log.info("Progress report: {} out of {}", i, steps);
                currentStep++;
            }
        }

        log.info("Table prepared! Starting test...");

        logProgressStep = performanceTestsProperties.delayTest().retries() / 10;
        currentStep = 1;

        for (int i = 0; i < performanceTestsProperties.delayTest().retries(); i++) {
            long start = System.currentTimeMillis();
            connection.createStatement().execute("SELECT * FROM test");
            long end = System.currentTimeMillis();
            ret.add(end - start);

            if (i == logProgressStep * currentStep) {
                log.info("Progress report: {} out of {}", i, performanceTestsProperties.delayTest().retries());
                currentStep++;
            }
        }

        connection.createStatement().executeUpdate("DROP TABLE test");

        connection.close();

        return ret;
    }
}
