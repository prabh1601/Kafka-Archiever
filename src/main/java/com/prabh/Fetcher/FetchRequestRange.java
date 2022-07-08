package com.prabh.Fetcher;

import java.util.Calendar;
import java.util.List;

public class FetchRequestRange {
    final List<Integer> currentValue;
    Long epoch;

    private FetchRequestRange(StartTimestampBuilder builder) {
        this.currentValue = builder.timestampParameters;
        this.epoch = builder.epoch;
    }

    private FetchRequestRange(EndTimestampBuilder builder) {
        this.currentValue = builder.timestampParameters;
        this.epoch = builder.epoch;
    }

    public static class StartTimestampBuilder {
        List<Integer> timestampParameters;
        public long epoch = -1L;

        public StartTimestampBuilder(long epochInMillis) {
            this.epoch = epochInMillis;
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(epochInMillis);
            this.timestampParameters = List.of(-1, c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1,
                    c.get(Calendar.DAY_OF_MONTH),
                    c.get(Calendar.HOUR_OF_DAY),
                    c.get(Calendar.MINUTE));
        }

        public FetchRequestRange build() {
            return new FetchRequestRange(this);
        }

        // Maybe removed if required
        public StartTimestampBuilder(int year, int month, int date, int hour, int min) {
            timestampParameters = List.of(-1, year, month, date, hour, min);
        }

        public StartTimestampBuilder(int year, int month, int date, int hour) {
            this(year, month, date, hour, 0);
        }

        public StartTimestampBuilder(int year, int month, int date) {
            this(year, month, date, 0);
        }

        public StartTimestampBuilder(int year, int month) {
            this(year, month, 0);
        }


    }

    public static class EndTimestampBuilder {
        List<Integer> timestampParameters;
        public long epoch = -1L;

        public EndTimestampBuilder(long epochInMillis) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(epochInMillis);
            this.epoch = epochInMillis;
            this.timestampParameters = List.of(-1, c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1,
                    c.get(Calendar.DAY_OF_MONTH),
                    c.get(Calendar.HOUR_OF_DAY),
                    c.get(Calendar.MINUTE));
        }

        public FetchRequestRange build() {
            return new FetchRequestRange(this);
        }

        // Maybe removed if required
        public EndTimestampBuilder(int year, int month, int date, int hour, int min) {
            timestampParameters = List.of(-1, year, month, date, hour, min);
        }

        public EndTimestampBuilder(int year, int month, int date, int hour) {
            this(year, month, date, hour, 59);
        }

        public EndTimestampBuilder(int year, int month, int date) {
            this(year, month, date, 23);
        }

        public EndTimestampBuilder(int year, int month) {
            this(year, month, 31);
        }
    }

    public String getStamp() {
        if (epoch != -1) return Long.toString(epoch);
        StringBuilder ret = new StringBuilder("");
        for (int i = 1; i <= 5; i++) {
            ret.append(currentValue.get(i));
            if (i != 5) ret.append(".");
        }
        return ret.toString();
    }
}
