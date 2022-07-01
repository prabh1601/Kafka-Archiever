package com.prabh.Fetcher;

class FilePaths {
    final long prefixEpoch;
    final long suffixEpoch;
    final String storeLocation = System.getProperty("java.io.tmpdir");
    final String localCacheDirectory;
    final String NetObjectListFile;
    final String DownloadedObjectListFile;
    final String ProducedObjectListFile;
    final String DownloadDirectory;
    final String RejectedDirectory;
    static final String POISON_PILL = "END OF COMMUNICATION";


    public FilePaths(long prefix, long suffix) {
        this.prefixEpoch = prefix;
        this.suffixEpoch = suffix;

        localCacheDirectory = String.format("%s/S3ToKafkaCache/%d-%d", storeLocation, prefixEpoch, suffixEpoch);
        DownloadDirectory = localCacheDirectory + "/Download";
        RejectedDirectory = localCacheDirectory + "/Rejected";
        NetObjectListFile = localCacheDirectory + "/NetObjects";
        DownloadedObjectListFile = localCacheDirectory + "/DownloadedObjects";
        ProducedObjectListFile = localCacheDirectory + "/ProducedObjects";
    }
}
