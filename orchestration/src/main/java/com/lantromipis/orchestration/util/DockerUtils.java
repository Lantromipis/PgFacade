package com.lantromipis.orchestration.util;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.ContainerNetworkSettings;
import com.github.dockerjava.api.model.NetworkSettings;
import jakarta.enterprise.context.ApplicationScoped;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ApplicationScoped
public class DockerUtils {

    private static final BigDecimal NANO_MULTIPLIER = BigDecimal.valueOf(1000000000);
    private static final String BYTES = "b";
    private static final String KILOBYTES = "k";
    private static final String MEGABYTES = "m";
    private static final String GIGABYTES = "g";
    private static final Pattern MEMORY_REGEX = Pattern.compile("^(\\d+)([bkmg])$");

    public String getContainerAddress(InspectContainerResponse inspectContainerResponse, String networkName) {
        return Optional.ofNullable(inspectContainerResponse)
                .map(InspectContainerResponse::getNetworkSettings)
                .map(NetworkSettings::getNetworks)
                .map(n -> n.get(networkName))
                .map(ContainerNetwork::getIpAddress)
                .orElse(null);
    }

    public String getContainerAddress(Container container, String networkName) {
        return Optional.of(container)
                .map(Container::getNetworkSettings)
                .map(ContainerNetworkSettings::getNetworks)
                .map(map -> map.get(networkName))
                .map(ContainerNetwork::getIpAddress)
                .orElse(null);
    }

    public String createUniqueObjectName(String start) {
        return start + "-" + UUID.randomUUID();
    }

    public String createUniqueObjectName(String start, String uniquePostfix) {
        return start + "-" + uniquePostfix;
    }

    public Long getNanoCpusFromDecimalCpus(BigDecimal decimalCpus) {
        if (decimalCpus == null) {
            return null;
        }

        return decimalCpus.multiply(NANO_MULTIPLIER)
                .longValue();
    }

    public Long getMemoryBytesFromString(String memoryString) {
        Matcher matcher = MEMORY_REGEX.matcher(memoryString);
        if (!matcher.matches()) {
            return null;
        }

        long memory = Long.parseLong(matcher.group(1));

        switch (matcher.group(2)) {
            case KILOBYTES -> memory = memory * 1024;
            case MEGABYTES -> memory = memory * 1024 * 1024;
            case GIGABYTES -> memory = memory * 1024 * 1024 * 1024;
        }

        return memory;
    }

    public boolean validateMemoryString(String memoryString) {
        Matcher matcher = MEMORY_REGEX.matcher(memoryString);
        return matcher.matches();
    }

    public List<String> getPossibleMemoryUnits() {
        return List.of(BYTES, KILOBYTES, MEGABYTES, GIGABYTES);
    }

    public String createEnvValueForRequest(String varName, String value) {
        return varName + "=" + value;
    }
}
