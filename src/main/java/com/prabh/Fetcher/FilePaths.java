package com.prabh.Fetcher;

public class FilePaths {
    final String prefixEpoch;
    final String suffixEpoch;
    final String storeLocation = System.getProperty("java.io.tmpdir");
    final String localCacheDirectory;
    final String NetObjectListFile;
    final String DownloadedObjectListFile;
    final String ProducedObjectListFile;
    final String DownloadDirectory;
    final String RejectedDirectoryPermanent;
    final String RejectedDirectoryTransient;
    static final String POISON_PILL = "END OF COMMUNICATION";


    public FilePaths(String prefix, String suffix) {
        this.prefixEpoch = prefix;
        this.suffixEpoch = suffix;

        localCacheDirectory = String.format("%s/S3ToKafkaCache/%s - %s", storeLocation, prefixEpoch, suffixEpoch);
        DownloadDirectory = localCacheDirectory + "/Download";
        RejectedDirectoryTransient = localCacheDirectory + "/Rejected/Transient";
        RejectedDirectoryPermanent = localCacheDirectory + "/Rejected/Permanent";
        NetObjectListFile = localCacheDirectory + "/NetObjects";
        DownloadedObjectListFile = localCacheDirectory + "/DownloadedObjects";
        ProducedObjectListFile = localCacheDirectory + "/ProducedObjects";
    }
}
