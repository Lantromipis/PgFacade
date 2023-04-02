package com.lantromipis.perfomance;

import com.lantromipis.PerformanceTestsProperties;
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
            String jdbcUrl = "jdbc:postgresql://"
                    + performanceTestsProperties.postgresConf().host()
                    + ":"
                    + performanceTestsProperties.postgresConf().port()
                    + "/"
                    + performanceTestsProperties.postgresConf().database();

            String username = performanceTestsProperties.postgresConf().user();
            String password = performanceTestsProperties.postgresConf().password();


            // get connection to fill pool with 1 connection
            Connection connection = DriverManager.getConnection(
                    jdbcUrl,
                    username,
                    password
            );
            connection.close();

            log.info("Running aquire connection test. Will connect to Postgres {} times an measure mean acquire connection time.", performanceTestsProperties.testAquireConnectionTime().retries());

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

            log.info("Average time to acquire connection after {} probes: {} ms", performanceTestsProperties.testAquireConnectionTime().retries(), results.stream().mapToLong(i -> i).average().getAsDouble());

        } catch (Exception e) {
            log.error("Unable to run aquire connection time tests! ", e);
        }
    }
}
