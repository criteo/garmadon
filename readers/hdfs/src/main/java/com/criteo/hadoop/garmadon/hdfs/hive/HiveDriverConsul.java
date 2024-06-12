package com.criteo.hadoop.garmadon.hdfs.hive;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.health.HealthServicesRequest;
import com.ecwid.consul.v1.health.model.HealthService;
import org.apache.hive.jdbc.HiveDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class HiveDriverConsul extends HiveDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveDriverConsul.class);

    static {
        try {
            java.sql.DriverManager.registerDriver(new HiveDriverConsul());
        } catch (SQLException e) {
            LOGGER.error("Issue to register " + HiveDriverConsul.class, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Fetches healthy service nodes
     */
    private List<HealthService> getHealthyEndPoints(String serviceName) {
        ConsulClient client = new ConsulClient("localhost");

        HealthServicesRequest request = HealthServicesRequest.newBuilder()
            .setPassing(true)
            .setQueryParams(QueryParams.DEFAULT)
            .build();
        Response<List<HealthService>> healthyServices = client.getHealthServices(serviceName, request);

        return healthyServices.getValue();
    }

    /**
     * Fetches one of the healthy node
     */
    private String getEndPoint(String url) throws SQLException {
        String serviceName = url.split("/")[0];
        List<HealthService> healthServices = getHealthyEndPoints(serviceName);
        if (healthServices.size() == 0) throw new SQLException("No nodes are available for service: " + serviceName);
        HealthService randomHealthyService = healthServices.get(ThreadLocalRandom.current().nextInt(healthServices.size()));


        String nodeHost = randomHealthyService.getNode().getAddress();
        String serviceHost = randomHealthyService.getService().getAddress();

        String host;
        if (serviceHost.isEmpty()) {
            host = nodeHost;
        } else {
            host = serviceHost;
        }

        String port = String.valueOf(randomHealthyService.getService().getPort());

        String hiveJdbcConf = url.replace(serviceName, "");
        return "jdbc:hive2://" + host + ":" + port + hiveJdbcConf;
    }

    public Connection connect(String url, Properties info) throws SQLException {
        String urlHive = getEndPoint(url);
        LOGGER.info("Try to connect to {}", urlHive);
        return super.connect(urlHive, info);
    }

}
