package com.lantromipis.perfomance;

import com.lantromipis.PerformanceTestsProperties;
import com.lantromipis.utils.MathUtils;
import com.lantromipis.utils.PostgresUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

@Slf4j
@ApplicationScoped
public class LoadTest {

    @Inject
    PerformanceTestsProperties performanceTestsProperties;

    @Inject
    ManagedExecutor managedExecutor;

    public void runTest() {
        Map<Integer, List<Long>> pgTest = null, pgFacadeTest = null, pgBouncerTest = null;
        try {
            log.info("Starting load test for original Postgres. Test plan: measure start time -> create connection -> execute select * -> close connection -> measure end time");
            pgTest = runTest(performanceTestsProperties.originalPostgresHost());

            log.info("Starting load test for PgFacade. Test plan: measure start time -> create connection -> execute select * -> close connection -> measure end time");
            pgFacadeTest = runTest(performanceTestsProperties.pgFacadeHost());

            if (performanceTestsProperties.runPgBouncer()) {
                log.info("Starting load test for PgBouncer. Test plan: measure start time -> create connection -> execute select * -> close connection -> measure end time");
                pgBouncerTest = runTest(performanceTestsProperties.pgBouncerHost());
            }

            log.info("Load test results for original Postgres after {} SELECT retries for table of size {} kb :",
                    performanceTestsProperties.delayTest().retries(),
                    performanceTestsProperties.delayTest().tableSizeKb()
            );

            log.info(getLogString(pgTest));

            log.info("Load test results for PgFacade proxy after {} SELECT retries for table of size {} kb :",
                    performanceTestsProperties.delayTest().retries(),
                    performanceTestsProperties.delayTest().tableSizeKb()
            );

            log.info(getLogString(pgFacadeTest));

            if (performanceTestsProperties.runPgBouncer() && pgBouncerTest != null) {
                log.info("Load test results for PgBouncer after {} SELECT retries for table of size {} kb :",
                        performanceTestsProperties.delayTest().retries(),
                        performanceTestsProperties.delayTest().tableSizeKb()
                );

                log.info(getLogString(pgBouncerTest));
            }
        } catch (Exception e) {
            log.error("Exception during load test!", e);
        }
    }

    private String getLogString(Map<Integer, List<Long>> pgTest) {
        StringBuilder stringBuilder = new StringBuilder();
        for (var entry : pgTest.entrySet()) {
            stringBuilder.append("\n").append("Num of connections: ")
                    .append(entry.getKey())
                    .append(" math expected value: ")
                    .append(MathUtils.calculateExpectedValue(entry.getValue()))
                    .append(" ms, min: ")
                    .append(MathUtils.calculateMinValue(entry.getValue()))
                    .append(" ms max: ")
                    .append(MathUtils.calculateMaxValue(entry.getValue()))
                    .append(" ms");
        }

        return stringBuilder.toString();
    }

    private Map<Integer, List<Long>> runTest(PerformanceTestsProperties.HostProperties hostProperties) throws Exception {
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

        Map<Integer, List<Long>> ret = new TreeMap<>();

        connection.createStatement().executeUpdate("DROP TABLE IF EXISTS test");
        connection.createStatement().executeUpdate("CREATE TABLE test(id bigserial, data varchar(32))");
        log.info("Preparing table for load test...");

        int steps = performanceTestsProperties.loadTest().tableSizeKb() * 1024 / 32;

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

        preparedStatement.close();

        log.info("Table prepared! Starting test...");

        steps = performanceTestsProperties.loadTest().retries();
        logProgressStep = performanceTestsProperties.loadTest().retries() / 10;
        currentStep = 1;

        log.info("Running tests for {} connections", performanceTestsProperties.loadTest().firstNumOfConnections());
        List<Long> temp1 = new ArrayList<>();

        for (int i = 0; i < performanceTestsProperties.loadTest().retries(); i++) {
            temp1.addAll(runForNumOfConnections(jdbcUrl, username, password, performanceTestsProperties.loadTest().firstNumOfConnections()));
            if (i == logProgressStep * currentStep) {
                log.info("Progress report: {} out of {}", i, steps);
                currentStep++;
            }
        }

        ret.put(performanceTestsProperties.loadTest().firstNumOfConnections(), temp1);

        logProgressStep = performanceTestsProperties.loadTest().retries() / 10;
        currentStep = 1;

        log.info("Running tests for {} connections", performanceTestsProperties.loadTest().secondNumOfConnections());
        List<Long> temp2 = new ArrayList<>();

        for (int i = 0; i < performanceTestsProperties.loadTest().retries(); i++) {
            temp2.addAll(runForNumOfConnections(jdbcUrl, username, password, performanceTestsProperties.loadTest().secondNumOfConnections()));
            if (i == logProgressStep * currentStep) {
                log.info("Progress report: {} out of {}", i, steps);
                currentStep++;
            }
        }

        ret.put(performanceTestsProperties.loadTest().secondNumOfConnections(), temp2);

        logProgressStep = performanceTestsProperties.loadTest().retries() / 10;
        currentStep = 1;

        log.info("Running tests for {} connections", performanceTestsProperties.loadTest().thirdNumOfConnections());
        List<Long> temp3 = new ArrayList<>();

        for (int i = 0; i < performanceTestsProperties.loadTest().retries(); i++) {
            temp3.addAll(runForNumOfConnections(jdbcUrl, username, password, performanceTestsProperties.loadTest().thirdNumOfConnections()));
            if (i == logProgressStep * currentStep) {
                log.info("Progress report: {} out of {}", i, steps);
                currentStep++;
            }
        }

        ret.put(performanceTestsProperties.loadTest().thirdNumOfConnections(), temp3);

        connection.createStatement().executeUpdate("DROP TABLE test");

        connection.close();

        return ret;
    }

    private List<Long> runForNumOfConnections(String jdbcUrl, String username, String password, int numOfConnections) throws Exception {
        List<Long> ret = new ArrayList<>();

        List<CompletableFuture<Long>> futures = new ArrayList<>();

        for (int j = 0; j < numOfConnections; j++) {
            futures.add(
                    managedExecutor.supplyAsync(() -> {
                        try {
                            long start = System.currentTimeMillis();
                            Connection testConn = DriverManager.getConnection(
                                    jdbcUrl,
                                    username,
                                    password
                            );

                            testConn.createStatement().execute("SELECT * FROM test");
                            testConn.close();

                            long end = System.currentTimeMillis();
                            return end - start;
                        } catch (Exception e) {
                            log.error("Error during load test ", e);
                            return -1L;
                        }
                    })
            );
        }

        for (var f : futures) {
            long value = f.get();
            if (value != -1L) {
                ret.add(value);
            }
        }

        return ret;
    }
}
