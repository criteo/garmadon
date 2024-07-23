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

/**
 * Create a connection based on a Consul service name.
 * In case of connection drop, it will fetch again healthy instances of garmadon agent.
 */
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

    /**
     * Fetches healthy garmadon forwarder end points from consul.
     */
    private List<HealthService> getHealthyEndPoints() {
        ConsulClient client = new ConsulClient("localhost");

        HealthServicesRequest request = HealthServicesRequest.newBuilder()
                .setPassing(true)
                .setQueryParams(QueryParams.DEFAULT)
                .build();
        Response<List<HealthService>> healthyServices = client.getHealthServices(serviceName, request);

        return healthyServices.getValue();
    }

    /**
     * Choose randomly a healthy garmadon forwarder and connect to it via a FixedConnection.
     */
    @Override
    public void establishConnection() {
        for (; ; ) {
            List<HealthService> healthServices = getHealthyEndPoints();

            HealthService randomHealthyService = healthServices.get(ThreadLocalRandom.current().nextInt(healthServices.size()));

            String nodeHost = randomHealthyService.getNode().getAddress();
            String serviceHost = randomHealthyService.getService().getAddress();

            String host;
            if (serviceHost.isEmpty()) {
                host = nodeHost;
            } else {
                host = serviceHost;
            }
            Integer port = randomHealthyService.getService().getPort();

            if (underlying != null) {
                underlying.close();
            }

            underlying = new FixedConnection(host, port);

            boolean connected = underlying.establishConnectionOnce();
            if (connected) {
                return;
            }
        }
    }

    /**
     * @return true if underlying is not null and underlying is connected.
     */
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
