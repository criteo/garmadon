package com.criteo.hadoop.garmadon.flink;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import java.util.Map;

public class SimpleMetricGroup extends UnregisteredMetricsGroup {

  private Map<String, String> variables;

  public SimpleMetricGroup(Map<String, String> variables) {
    this.variables = variables;
  }

  @Override
  public Map<String, String> getAllVariables() {
    return variables;
  }

  @Override
  public String[] getScopeComponents() {
    return variables.values().stream().toArray(String[]::new);
  }
}
