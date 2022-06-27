package com.prabh.Utils;

import java.util.Calendar;
import java.util.List;

public class FetchRangeEpoch {
    List<Integer> timestampParameters;
    long epoch;

    public FetchRangeEpoch(long epochInMillis) {
        this.epoch = epochInMillis;
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(epochInMillis);
        this.timestampParameters = List.of(-1, c.get(Calendar.YEAR),
                c.get(Calendar.MONTH) + 1,
                c.get(Calendar.DAY_OF_MONTH),
                c.get(Calendar.HOUR_OF_DAY),
                c.get(Calendar.MINUTE));
    }
}
