package org.embulk.input.sftp;

import org.embulk.spi.unit.LocalFile;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

import java.util.Optional;

public interface PluginTask
        extends Task, FileList.Task
{
    @Config("host")
    String getHost();

    @Config("port")
    @ConfigDefault("22")
    int getPort();

    @Config("user")
    String getUser();

    @Config("password")
    @ConfigDefault("null")
    Optional<String> getPassword();

    @Config("secret_key_file")
    @ConfigDefault("null")
    Optional<LocalFile> getSecretKeyFile();
    void setSecretKeyFile(Optional<LocalFile> secretKeyFile);

    @Config("secret_key_passphrase")
    @ConfigDefault("\"\"")
    String getSecretKeyPassphrase();

    @Config("user_directory_is_root")
    @ConfigDefault("true")
    Boolean getUserDirIsRoot();

    @Config("timeout")
    @ConfigDefault("600") // 10 minutes
    int getSftpConnectionTimeout();

    @Config("max_connection_retry")
    @ConfigDefault("5") // 5 times retry to connect sftp server if failed.
    int getMaxConnectionRetry();

    @Config("path_prefix")
    String getPathPrefix();

    @Config("incremental")
    @ConfigDefault("true")
    boolean getIncremental();

    @Config("last_path")
    @ConfigDefault("null")
    Optional<String> getLastPath();

    @Config("proxy")
    @ConfigDefault("null")
    Optional<ProxyTask> getProxy();

    FileList getFiles();
    void setFiles(FileList files);
}
