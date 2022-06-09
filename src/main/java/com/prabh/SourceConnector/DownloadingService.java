package com.prabh.SourceConnector;

import java.util.ArrayList;
import java.util.List;

public class DownloadingService {
    private final RequestObject start;
    private final RequestObject end;
    private final int maxDepth = 5;
    private final List<Integer> maxPossible;
    private final List<String> levelPrefix;
    private final String downloadTopic;

    public DownloadingService(String _topic, RequestObject _start, RequestObject _end) {
        this.start = _start;
        this.end = _end;
        this.maxPossible = List.of(-1, Integer.MAX_VALUE, 12, 31, 23, 59);
        this.levelPrefix = List.of("topic", "year=", "month=", "day=", "hour=", "");
        this.downloadTopic = _topic;
    }

    public void stageForDownload(int depth, List<Integer> state) {
        // topics/test/year=2022/month=JUNE/day=6/hour=12/1654499813057-1
        StringBuilder key_prefix = new StringBuilder("topics/" + downloadTopic + "/");
        for (int i = 1; i <= depth; i++) {
            key_prefix.append(levelPrefix.get(i) + state.get(i));
            if (i != maxDepth) key_prefix.append("/");
        }

        // Pass to transfer manager
        System.out.println(key_prefix);
    }

    public void query(int currentDepth, List<Integer> currentState, boolean leftBorder, boolean rightBorder) {
        // leaf node or inside range -> download all files with same prefix
        if (currentDepth == maxDepth || (!leftBorder && !rightBorder)) {
            stageForDownload(currentDepth, currentState);
            return;
        }

        int leftEndpoint = start.currentValue.get(currentDepth);
        int rightEndpoint = end.currentValue.get(currentDepth);
        int startingValue = leftBorder ? leftEndpoint : 1;
        int endingValue = rightBorder ? rightEndpoint : maxPossible.get(currentDepth + 1);
        for (int i = startingValue; i <= endingValue; i++) {
            currentState.add(i);
            boolean nLeftBorder = leftBorder && (i == leftEndpoint);
            boolean nRightBorder = rightBorder && (i == rightEndpoint);
            query(currentDepth + 1, currentState, nLeftBorder, nRightBorder);
            currentState.remove(currentState.size() - 1);
            // Holy god you kidding me ? why is there no c++ like pop_back() ??????? :/
        }
    }

    public void run() {
        List<Integer> currentState = new ArrayList<>(maxDepth + 1);
        currentState.add(-1);
        query(0, currentState, true, true);
    }
}
