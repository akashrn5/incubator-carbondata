package org.apache.carbondata.core.util;

import java.io.Serializable;

public class MinMaxTime implements Serializable {
  private Long min;

  private Long max;

  MinMaxTime(Long min, Long max) {
    this.min = min;
    this.max = max;
  }

  public Long getMin() {
    return min;
  }

  public Long getMax() {
    return max;
  }
}
