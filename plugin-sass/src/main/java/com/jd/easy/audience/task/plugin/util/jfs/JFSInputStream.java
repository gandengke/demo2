package com.jd.easy.audience.task.plugin.util.jfs;

import com.google.common.base.Throwables;
import com.jd.jss.service.BucketService;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.jfs.JFSUtils;

@Private
@Unstable
class JFSInputStream extends FSInputStream {
    private final Path path;
    private final long maxSkipSize;
    private BucketService bucket;
    private boolean closed;
    private InputStream in;
    private long streamPosition;
    private long nextReadPosition;

    public JFSInputStream(BucketService bucket, Path path, long maxSkipSize) {
        this.bucket = bucket;
        this.path = path;
        this.maxSkipSize = maxSkipSize;
    }
    @Override
    public void close() {
        this.closed = true;
        this.closeStream();
    }
    @Override
    public void seek(long pos) throws IOException {
        if (!this.closed) {
            if (pos < 0L) {
                throw new IOException("Cannot seek after EOF");
            } else {
                this.nextReadPosition = pos;
            }
        }
    }
    @Override
    public long getPos() {
        return this.nextReadPosition;
    }
    @Override
    public int read() {
        throw new UnsupportedOperationException();
    }
    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        try {
            this.seekStream();
            int bytesRead = this.in.read(buffer, offset, length);
            if (bytesRead != -1) {
                this.streamPosition += (long) bytesRead;
                this.nextReadPosition += (long) bytesRead;
            }

            return bytesRead;
        } catch (Exception var5) {
            Throwables.propagateIfInstanceOf(var5, IOException.class);
            throw Throwables.propagate(var5);
        }
    }
@Override
    public boolean seekToNewSource(long targetPos) {
        return false;
    }

    private void seekStream() throws IOException {
        if (this.in == null || this.nextReadPosition != this.streamPosition) {
            if (this.in != null && this.nextReadPosition > this.streamPosition) {
                long skip = this.nextReadPosition - this.streamPosition;
                if (skip <= Math.max((long) this.in.available(), this.maxSkipSize)) {
                    try {
                        if (this.in.skip(skip) == skip) {
                            this.streamPosition = this.nextReadPosition;
                            return;
                        }
                    } catch (IOException var4) {
                    }
                }
            }

            this.streamPosition = this.nextReadPosition;
            this.closeStream();
            this.openStream();
        }
    }

    private void openStream() throws IOException {
        if (this.in == null) {
            this.in = this.openStream(this.path, this.nextReadPosition);
            this.streamPosition = this.nextReadPosition;
        }

    }

    private InputStream openStream(Path path, long start) throws IOException {
        return this.bucket.object(JFSUtils.pathToKey(path)).range(start).get().getInputStream();
    }

    private void closeStream() {
        if (this.in != null) {
            try {
                this.in.close();
            } catch (IOException var2) {
            }

            this.in = null;
        }

    }
}
