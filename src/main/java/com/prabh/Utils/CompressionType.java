package com.prabh.Utils;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public enum CompressionType {
    // Raw Plain Text
    NONE("none", ""),

    // Gzip
    GZIP("gzip", "gz") {
        private static final int GZIP_BUFFER_SIZE_BYTES = 8 * 1024;  // 8 KB

        @Override
        public OutputStream wrapOutputStream(OutputStream out) throws IOException {
            return wrapOutputStream(out, Deflater.DEFAULT_COMPRESSION);
        }

        @Override
        public OutputStream wrapOutputStream(OutputStream out, int level) throws IOException {
            return new GZIPOutputStream(out, GZIP_BUFFER_SIZE_BYTES) {
                public OutputStream setLevel(int level) {
                    def.setLevel(level);
                    return this;
                }
            }.setLevel(level);
        }

        @Override
        public InputStream wrapInputStream(InputStream in) throws IOException {
            return new GZIPInputStream(in);
        }
    },

    // Snappy
    SNAPPY("snappy", "snappy") {
        @Override
        public OutputStream wrapOutputStream(OutputStream out) throws IOException {
            return new SnappyOutputStream(out);
        }

        @Override
        public InputStream wrapInputStream(InputStream in) throws IOException {
            return new SnappyInputStream(in);
        }
    };

    // Add Any other Compression type
    // The current design is made upon the assumption that compression types support input/output streams.

    public final String name;
    public final String extension;

    public static CompressionType getCompressionType(String name) {
        name = name.toLowerCase();
        if (name.equals(NONE.name) || name.equals(NONE.extension)) {
            return NONE;
        } else if (name.equals(GZIP.name) || name.equals(GZIP.extension)) {
            return GZIP;
        } else if (name.equals(SNAPPY.name) || name.equals(SNAPPY.extension)) {
            return SNAPPY;
        } else {
            throw new IllegalArgumentException("""
                    The Asked Compression Type Is Unknown/Unsupported
                    Currently Supported Types - GZip, Snappy
                    """);
        }
    }

    CompressionType(String _name, String _extension) {
        this.name = _name;
        this.extension = _extension;
    }

    public OutputStream wrapOutputStream(OutputStream out) throws IOException {
        return out;
    }

    public OutputStream wrapOutputStream(OutputStream out, int level) throws IOException {
        return wrapOutputStream(out);
    }

    public InputStream wrapInputStream(InputStream in) throws IOException {
        return in;
    }
}
