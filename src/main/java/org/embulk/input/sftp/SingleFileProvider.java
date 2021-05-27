package org.embulk.input.sftp;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.embulk.util.file.InputStreamFileInput;
import org.embulk.util.file.InputStreamFileInput.InputStreamWithHints;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Iterator;

public class SingleFileProvider
        implements InputStreamFileInput.Provider
{
    private final StandardFileSystemManager manager;
    private final FileSystemOptions fsOptions;
    private final Iterator<String> iterator;
    private final int maxConnectionRetry;
    private boolean opened = false;
    private final Logger log = LoggerFactory.getLogger(SingleFileProvider.class);

    public SingleFileProvider(PluginTask task, int taskIndex, StandardFileSystemManager manager, FileSystemOptions fsOptions)
    {
        this.manager = manager;
        this.fsOptions = fsOptions;
        this.iterator = task.getFiles().get(taskIndex).iterator();
        this.maxConnectionRetry = task.getMaxConnectionRetry();
    }

    @Override
    public InputStreamWithHints openNextWithHints() throws IOException
    {
        if (opened || !iterator.hasNext()) {
            return null;
        }
        opened = true;
        final String key = iterator.next();

        try {
            return RetryExecutor.builder()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWaitMillis(500)
                    .withMaxRetryWaitMillis(30 * 1000)
                    .build()
                    .runInterruptible(new Retryable<InputStreamWithHints>() {
                        @Override
                        public InputStreamWithHints call() throws FileSystemException
                        {
                            FileObject file = manager.resolveFile(key, fsOptions);
                            return new InputStreamWithHints(
                                    file.getContent().getInputStream(), file.getPublicURIString());
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                throws RetryGiveupException
                        {
                            if (exception.getMessage().indexOf("Permission denied") > 0) {
                                log.error("Could not download file due to Permission Denied");
                                throw new RetryGiveupException(exception);
                            }
                            String message = String.format("SFTP GET request failed. Retrying %d/%d after %d seconds. Message: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                            if (retryCount % 3 == 0) {
                                log.warn(message, exception);
                            }
                            else {
                                log.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException)
                                throws RetryGiveupException
                        {
                        }
                    });
        }
        catch (RetryGiveupException ex) {
            throw new RuntimeException(ex.getCause());
        }
        catch (InterruptedException ex) {
            throw new InterruptedIOException();
        }
    }

    @Override
    public void close()
    {
        if (manager != null) {
            manager.close();
        }
    }
}
