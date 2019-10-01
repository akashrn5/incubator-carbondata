package org.apache.carbondata.core.util;

import java.util.ArrayList;
import java.util.List;

public class TimestampMinMaxStats {

  private TimestampMinMaxStats() {
  }

  public static TimestampMinMaxStats getInstance() {
    return timestampMinMaxStats;
  }

  private List<MinMaxTime> timeStampMinList = new ArrayList<>();


  private static final TimestampMinMaxStats timestampMinMaxStats = new TimestampMinMaxStats();


  public List<MinMaxTime> getTimeStampMinList() {
    return timeStampMinList;
  }

  public void setTimeStampMinList(Long min, Long max) {
    this.timeStampMinList.add(new MinMaxTime(min, max));
  }

}

