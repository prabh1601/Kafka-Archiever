package com.prabh.Fetcher;

import java.util.Calendar;
import java.util.List;

public class FetchRequest {
    private static int depth = 5;
    final List<Integer> currentValue;

    private FetchRequest(StartTimestampBuilder builder) {
        this.currentValue = builder.timestampParameters;
    }

    private FetchRequest(EndTimestampBuilder builder) {
        this.currentValue = builder.timestampParameters;
    }

    public static class StartTimestampBuilder {
        List<Integer> timestampParameters;

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

        public StartTimestampBuilder(int epochInMillis) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(epochInMillis);
            this(c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1,
                    c.get(Calendar.DAY_OF_MONTH),
                    c.get(Calendar.HOUR_OF_DAY),
                    c.get(Calendar.MINUTE));
        }

        public FetchRequest build() {
            return new FetchRequest(this);
        }
    }

    public static class EndTimestampBuilder {
        List<Integer> timestampParameters;

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

        public EndTimestampBuilder(int year) {
            this(year, 12);
        }

        public FetchRequest build() {
            return new FetchRequest(this);
        }

    }


}
