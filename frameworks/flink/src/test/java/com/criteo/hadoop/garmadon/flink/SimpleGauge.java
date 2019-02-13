package com.criteo.hadoop.garmadon.flink;

import org.apache.flink.metrics.Gauge;

public class SimpleGauge implements Gauge<Long> {

  private Long value;

  public SimpleGauge(Long value) {
    this.value = value;
  }

  public void setValue(Long value) {
    this.value = value;
  }

  @Override
  public Long getValue() {
    return value;
  }
}
