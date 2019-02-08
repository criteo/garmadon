package com.criteo.hadoop.garmadon.agent;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.health.HealthServicesRequest;
import com.ecwid.consul.v1.health.model.HealthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ConsulConnection implements Connection {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsulConnection.class);

    private final String serviceName;
    private FixedConnection underlying;

    public ConsulConnection(String name) {
        this.serviceName = name;
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        if (underlying == null) {
            throw new IllegalStateException("");
        }
        underlying.write(bytes);
    }

    @Override
    public int read(byte[] buf) throws IOException {
        if (underlying == null) {
            throw new IllegalStateException("");
        }
        return underlying.read(buf);
    }

    private List<HealthService> getHealthyEndPoints() {
        ConsulClient client = new ConsulClient("localhost");

        HealthServicesRequest request = HealthServicesRequest.newBuilder()
                .setPassing(true)
                .setQueryParams(QueryParams.DEFAULT)
                .build();
        Response<List<HealthService>> healthyServices = client.getHealthServices(serviceName, request);

        return healthyServices.getValue();
    }

    @Override
    public void establishConnection() {
        List<HealthService> nodes = getHealthyEndPoints();

        HealthService electedNode = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));

        String host = electedNode.getNode().getAddress();
        Integer port = electedNode.getService().getPort();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("will use forwarder at " + host + ":" + port);
        }

        if (underlying != null) {
            underlying.close();
        }

        underlying = new FixedConnection(host, port);

        underlying.establishConnection();
    }

    @Override
    public boolean isConnected() {
        return underlying != null && underlying.isConnected();
    }

    @Override
    public void close() {
        if (underlying != null) {
            underlying.close();
        }
    }
}
