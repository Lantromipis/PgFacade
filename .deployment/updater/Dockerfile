FROM eclipse-temurin:21.0.1_12-jdk-jammy

ENV LANGUAGE='en_US:en'

SHELL ["/bin/bash", "-c"]

# We make four distinct layers so if there are application changes the library layers can be re-used
COPY --chown=root target/quarkus-app/lib/ /deployments/lib/
COPY --chown=root target/quarkus-app/*.jar /deployments/
COPY --chown=root target/quarkus-app/app/ /deployments/app/
COPY --chown=root target/quarkus-app/quarkus/ /deployments/quarkus/

# docker.sock permission denied. To fix that, id of users in host and container must match
USER root

ENV JAVA_OPTS="-Dquarkus.http.host=0.0.0.0 -Djava.util.logging.manager=org.jboss.logmanager.LogManager"
ENV JAVA_APP_JAR="/deployments/quarkus-run.jar"

ENTRYPOINT ["/bin/bash", "-c", "java ${JAVA_OPTS} -jar ${JAVA_APP_JAR}"]


