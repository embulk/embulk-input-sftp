## 0.3.5 - 2021-02-02
* [maintenance] Support OpenSsh key

## 0.3.4 - 2020-10-09
* [maintenance] Update commons vfs2 latest version (2.6.0) to fix timeout setting not working

## 0.3.3 - 2020-02-14
* [maintenance] Build with the "org.embulk.embulk-plugins" Gradle plugin [#42](https://github.com/embulk/embulk-input-sftp/pull/42)
## 0.3.2 - 2019-07-25
* [maintenance] Do not retry when "Connection refused" is returned and throw ConfigException [#40](https://github.com/embulk/embulk-input-sftp/pull/40)

## 0.3.1 - 2019-01-08
* [maintenance] Hide password shown in log when parse failure happens [#39](https://github.com/embulk/embulk-input-sftp/pull/39)

## 0.3.0 - 2018-12-26
[new feature] Use file name output feature in Embulk core to show file name in cmdout [#37](https://github.com/embulk/embulk-input-sftp/pull/37)

## 0.2.12 - 2018-10-05
* [maintenance] Give up processing and throw ConfigException when "Auth fail" first appears [#33](https://github.com/embulk/embulk-input-sftp/pull/33)

## 0.2.11 - 2018-05-07
* [maintenance] Use java.util.regex.Pattern for host name validation [#32](https://github.com/embulk/embulk-input-sftp/pull/32)

## 0.2.10 - 2018-04-26
* [maintenance] Add validation for "host" and "proxy.host" [#31](https://github.com/embulk/embulk-input-sftp/pull/31)

## 0.2.9 - 2018-04-20
* [maintenance] Throw ConfigException when process fails with "com.jcraft.jsch.JSchException: Auth fail" [#29](https://github.com/embulk/embulk-input-sftp/pull/29) 

## 0.2.8 - 2018-03-12
* [maintenance] Fix SFTP non existent last_path [#28](https://github.com/embulk/embulk-input-sftp/pull/28)

## 0.2.7 - 2018-03-02
* [maintenance] Fix SFTP connection remaining problem [#27](https://github.com/sakama/embulk-input-sftp/pull/27)

## 0.2.6 - 2018-01-15
- [maintenance] Upgrade "commons-vfs2", "com.jcraft:jsch" and "commons-io:commons-io"
  - https://github.com/embulk/embulk-input-sftp/pull/25

## 0.2.5 - 2017-06-20

* [maintenance] Fix bug path_prefix extension are removed [#23](https://github.com/sakama/embulk-input-sftp/pull/23)

## 0.2.4 - 2017-06-05

* [maintenance] Improve logic for remote file search with path_prefix  [#22](https://github.com/sakama/embulk-input-sftp/pull/22)

## 0.2.3 - 2016-09-30

* [maintenance] Fix auth failure while generating last_path under limited case [#20](https://github.com/sakama/embulk-input-sftp/pull/20)

## 0.2.2 - 2016-09-26

* [maintenance] Fix bug Use second as timetout setting instead of milli second [#18](https://github.com/embulk/embulk-input-sftp/pull/18)

## 0.2.1 - 2016-09-12
* [maintenance] Fix last_path generation failure when password contains special chars [#15](https://github.com/sakama/embulk-input-sftp/pull/15)

## 0.2.0 - 2016-08-19

* [new feature] Support incremental option [#11](https://github.com/sakama/embulk-input-sftp/pull/11)
* [maintenance] Mask password in the log [#12](https://github.com/sakama/embulk-input-sftp/pull/12)
* [maintenance] Fix file listing order and handling of last_path [#14](https://github.com/sakama/embulk-input-sftp/pull/14)

## 0.1.2 - 2015-03-23

* [maintenance] Use RetryExecutor when retrying that is provide by embulk-core [#9](https://github.com/sakama/embulk-input-sftp/pull/9)

## 0.1.1 - 2015-03-18

* [new feature] Support last_path_ option [#2](https://github.com/sakama/embulk-input-sftp/pull/2)[#4](https://github.com/sakama/embulk-input-sftp/pull/4)[#7](https://github.com/sakama/embulk-input-sftp/pull/7)
* [new feature] Support path_match_pattern option [#6](https://github.com/sakama/embulk-input-sftp/pull/6)
* [maintenance] Add unit test [#3](https://github.com/sakama/embulk-input-sftp/pull/3)
* [maintenance] Skip retry of file downloading when permission denied error happens [#1](https://github.com/sakama/embulk-input-sftp/pull/1)
