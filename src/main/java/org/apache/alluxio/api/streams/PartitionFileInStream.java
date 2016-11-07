package org.apache.alluxio.api.streams;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by weijia.liu
 * Date :  2016/11/6.
 * Time :  21:21
 */
public class PartitionFileInStream extends InputStream{
    private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

    private FileSystem fs;
    private FileInStream inStream;
    private String filePath;
    private OpenFileOptions options;
    private long startPos;
    private long length;
    private long currentPos;

    public PartitionFileInStream(FileSystem fs, String filePath, OpenFileOptions options, long startPos, long length) {
        this.fs = fs;
        this.filePath = filePath;
        this.options = options;
        this.startPos = startPos;
        this.length = length;
    }

    private void init() {
        try {
            inStream = fs.openFile(new AlluxioURI(filePath), options);
            inStream.skip(startPos);
            currentPos = startPos;
        } catch (IOException e) {
            LOG.error("{}", e);
        } catch (AlluxioException e) {
            LOG.error("{}", e);
        }
    }

    public int read() throws IOException {
        throw new IOException("this method is not supported");
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (inStream == null) {
            init();
        }

        if (currentPos >= (startPos + length)) {
            return -1;
        } else {
            if (currentPos + len > startPos + length) {
                len = (int) (startPos + length - currentPos);
            }
        }

        int read = inStream.read(b, off, len);
        currentPos += read;
        return read;
    }

    @Override
    public void close() throws IOException {
        if (inStream != null) {
            inStream.close();
        }
    }
}
