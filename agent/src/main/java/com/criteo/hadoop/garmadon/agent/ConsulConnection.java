package com.criteo.hadoop.garmadon.agent;

import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.model.health.Node;
import com.orbitz.consul.model.health.ServiceHealth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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

    private List<ServiceHealth> getHealthyEndPoints() {
        Consul client = Consul.builder().build();
        HealthClient healthClient = client.healthClient();
        List<ServiceHealth> nodes = healthClient.getHealthyServiceInstances(serviceName).getResponse();

        client.destroy();

        return nodes;
    }

    @Override
    public void establishConnection() {
        List<ServiceHealth> nodes = getHealthyEndPoints();
        ServiceHealth electedNode = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));

        Node node = electedNode.getNode();
        URI uri = null;
        try {
            uri = new URI(node.getAddress());
        } catch (URISyntaxException e) {
            LOGGER.error(e.getMessage(), e);
        }

        if (underlying != null) {
            underlying.close();
        }

        underlying = new FixedConnection(uri.getHost(), uri.getPort());

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
