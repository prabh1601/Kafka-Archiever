package com.prabh.SourceConnector;

import com.amazonaws.Request;

import java.util.List;
import java.util.stream.IntStream;

public class RequestObject {
    private static int depth = 5;
    final List<Integer> currentValue;

    private RequestObject(StartTimestampBuilder builder) {
        this.currentValue = builder.timestampParameters;
    }

    private RequestObject(EndTimestampBuilder builder) {
        this.currentValue = builder.timestampParameters;
    }

    public static class StartTimestampBuilder {
        List<Integer> timestampParameters;

        public StartTimestampBuilder(int year, int month, int date, int hour, int min) {
            timestampParameters = List.of(year, month, date, hour, min);
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

        public StartTimestampBuilder(int year) {
            this(year, 0);
        }

        public RequestObject build() {
            return new RequestObject(this);
        }
    }

    public static class EndTimestampBuilder {
        List<Integer> timestampParameters;

        public EndTimestampBuilder(int year, int month, int date, int hour, int min) {
            timestampParameters = List.of(year, month, date, hour, min);
        }

        public EndTimestampBuilder(int year, int month, int date, int hour) {
            this(year, month, date, hour, 0);
        }

        public EndTimestampBuilder(int year, int month, int date) {
            this(year, month, date, 0);
        }

        public EndTimestampBuilder(int year, int month) {
            this(year, month, 0);
        }

        public EndTimestampBuilder(int year) {
            this(year, 0);
        }

        public RequestObject build() {
            return new RequestObject(this);
        }

    }


}
