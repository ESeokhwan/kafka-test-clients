package org.example;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.ThreadContext;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Slf4j
public class FileReadChecker implements Runnable {

  @Getter
  @Parameters(index = "0", description = "Path of file to read")
  private String filePath;

  @Getter
  @Option(names = {"-s", "--size"}, description = "Total size to read")
  private long size = 1024*1024;

  public FileReadChecker() {
    super();
  }

  public static void main(String[] args) {
    RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
    String pid = rt.getName();
    ThreadContext.put("PID", pid);

    new CommandLine((new FileReadChecker()))
        .execute(args);

    log.info("DONE");
  }

  @Override
  public void run() {
    long timeDiff = System.currentTimeMillis() * 1_000_000 - System.nanoTime();
    long startTime;
    long endTime;
    Path devNullPath = Paths.get("/dev/null");
    Path srcFilePath = Paths.get(filePath);
    try {
      FileChannel devNullChannel = FileChannel.open(
          devNullPath,
          StandardOpenOption.WRITE
      );
      FileChannel fileChannel = FileChannel.open(
          srcFilePath,
          StandardOpenOption.CREATE,
          StandardOpenOption.READ
      );
      startTime = System.nanoTime();
      devNullChannel.transferFrom(fileChannel, 0, size);
      endTime = System.nanoTime();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    log.info("Start Time: " + new Date((startTime + timeDiff) / 1_000_000) + ", End Time: " + new Date((endTime + timeDiff) / 1_000_000));
    log.info("transfer Time: " + ((endTime - startTime) / 1_000_000) + "ns");
  }
}
