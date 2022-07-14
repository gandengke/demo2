package com.jd.easy.audience.task.plugin.util.jfs;

import com.jd.jss.service.BucketService;

import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

@Private
@Unstable
class JFSOutputStream extends FilterOutputStream {
    public static final Log LOG = LogFactory.getLog(JFSOutputStream.class);

    private final BucketService bucket;

    private final String key;

    private final File tempFile;

    private boolean closed;

    public JFSOutputStream(BucketService bucket, String key, File tempFile) throws IOException {
        super(new BufferedOutputStream(new FileOutputStream(tempFile)));
        this.bucket = bucket;
        this.key = key;
        this.tempFile = tempFile;
    }

    @Override
    public void close() throws IOException {
        if (this.closed) {
            return;
        }
        this.closed = true;
        try {
            super.close();
            uploadObject();
        } finally {
            if (!this.tempFile.delete()) {
                LOG.warn("Could not delete temporary file: " + this.tempFile);
            }
        }
    }

    private void uploadObject() throws IOException {
        synchronized (this.bucket) {
            if (this.tempFile.length() > 200_000_000) {
                this.bucket.object(this.key).entity(this.tempFile).resumableUpload();
            } else {
                this.bucket.object(this.key).entity(this.tempFile).put();
            }
        }
    }
}
