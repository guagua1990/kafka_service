package com.liveramp.kafka_service.zookeeper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.InMemoryAlertsHandler;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.logging.LoggingHelper;
import com.rapleaf.support.thread.NamedThreadFactory;

public class ZookeeperServers {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperServers.class);

  private static final String DEFAULT_WORKING_DIR = "/tmp/zookeeper";
  private static final String DATA_DIR = "/data";
  private static final String LOG_DIR = "/log";

  private ZookeeperServers() {
    throw new AssertionError("Should not instantiate zookeeper servers");
  }

  public static Set<ExecutorService> startProductionZk(int id) throws IOException {
    ZookeeperEnv.ZKEnsembles server = null;
    switch (id) {
      case 0:
        server = ZookeeperEnv.ZKEnsembles.SERVER_0;
        break;
      case 1:
        server = ZookeeperEnv.ZKEnsembles.SERVER_1;
        break;
      case 2:
        server = ZookeeperEnv.ZKEnsembles.SERVER_2;
        break;
      case 3:
        server = ZookeeperEnv.ZKEnsembles.SERVER_3;
        break;
      case 4:
        server = ZookeeperEnv.ZKEnsembles.SERVER_4;
        break;
      default:
        throw new IllegalArgumentException("invalid zookeeper id " + id);
    }
    return Collections.singleton(startFromEnsembles(server, DEFAULT_WORKING_DIR, ZookeeperEnv.PRODUCTION_ZKS));

  }

  public static Set<ExecutorService> startTestZks() throws IOException {
    Set<ExecutorService> services = Sets.newHashSet();
    for (ZookeeperEnv.ZKEnsembles sever : ZookeeperEnv.TEST_ZKS) {
      services.add(startFromEnsembles(sever, DEFAULT_WORKING_DIR + "/zk" + sever.getId(), ZookeeperEnv.TEST_ZKS));
      System.out.println(String.format("Starting service %d on %s", sever.getId(), sever.getHostClientPort()));
    }
    return services;
  }

  public static ExecutorService startFromEnsembles(ZookeeperEnv.ZKEnsembles zookeeper, String workingDir, EnumSet<ZookeeperEnv.ZKEnsembles> ensembles) throws IOException {
    if (!ensembles.contains(zookeeper)) {
      throw new IllegalArgumentException("This ensemble " + zookeeper + " doesn't belong to the working set " + ensembles);
    }
    int id = zookeeper.getId();
    if (!verifyWorkingDir(id, workingDir)) {
      LOG.info("clear current working directory {} and set up new one for zookeeper {}", workingDir, id);
      File file = new File(workingDir);
      if (file.exists()) {
        FileUtils.deleteDirectory(file);
      }
      setupNewWorkingDir(id, workingDir);
    }

    final Builder serverBuilder = new Builder(zookeeper.getClientPort(), workingDir);
    for (ZookeeperEnv.ZKEnsembles server : ensembles) {
      serverBuilder.addServer(server.getId(), server.getHostPorts());
    }
    LOG.info("starting zookeeper {} in {}", id, workingDir);
    return serverBuilder.start();
  }

  public static ExecutorService startFromProperties(String propertiesPath) throws IOException {
    Properties prop = new Properties();
    InputStream in = null;
    try {
      in = new FileInputStream(propertiesPath);
      prop.load(in);
      return new Builder(prop).start();
    } catch (FileNotFoundException e) {
      LOG.error("The properties file {} doesn't exist", propertiesPath);
      if (in != null) {
        in.close();
      }
    }
    return null;
  }

  private static boolean verifyWorkingDir(int id, String workingDir) throws IOException {
    if (!isValidDir(workingDir)) {
      return false;
    }
    if (!isValidDir(workingDir + DATA_DIR)) {
      return false;
    }
    if (!isValidDir(workingDir + LOG_DIR)) {
      return false;
    }
    File myid = new File(workingDir + DATA_DIR + "/myid");
    return !(!myid.exists() || !myid.isFile()) && Files.readFirstLine(myid, Charset.forName("UTF-8")).equals(String.valueOf(id));
  }

  private static void setupNewWorkingDir(int id, String workingDir) throws IOException {
    makeDir(workingDir);
    makeDir(workingDir + LOG_DIR);
    File dataDir = makeDir(workingDir + DATA_DIR);

    File idFd = new File(dataDir, "myid");
    FileWriter fileWriter = new FileWriter(idFd);
    fileWriter.write(String.valueOf(id));
    fileWriter.close();
  }

  private static boolean isValidDir(String path) {
    File workDir = new File(path);
    return workDir.exists() && workDir.isDirectory();
  }

  private static File makeDir(String path) throws IOException {
    File file = new File(path);
    if (!file.mkdirs()) {
      throw new IOException("fail to mkdir " + path);
    }
    return file;
  }

  public static class Builder {
    private final Properties properties;

    public Builder(int clientPort, String workingDir) {
      this.properties = new Properties();
      setWorkingDir(workingDir);
      setClientPort(clientPort);
      setInitLimit(ZookeeperEnv.INIT_LIMIT);
      setSyncLimit(ZookeeperEnv.SYNC_LIMIT);
    }

    public Builder(Properties properties) {
      this.properties = properties;
    }

    private Builder setClientPort(int clientPort) {
      return setProperty("clientPort", clientPort);
    }

    private Builder setWorkingDir(String workingDir) {
      setProperty("dataDir", workingDir + DATA_DIR);
      setProperty("dataLogDir", workingDir + LOG_DIR);
      return this;
    }

    public Builder setInitLimit(int initLimit) {
      return setProperty("initLimit", initLimit);
    }

    public Builder setSyncLimit(int syncLimit) {
      return setProperty("syncLimit", syncLimit);
    }

    public Builder addServer(int id, String address) {
      return setProperty(String.format("server.%d", id), address);
    }

    private Builder setProperty(String key, String value) {
      this.properties.setProperty(key, value);
      return this;
    }

    private Builder setProperty(String key, int value) {
      this.properties.setProperty(key, String.valueOf(value));
      return this;
    }

    public ExecutorService start() {
      ExecutorService service = Executors.newSingleThreadExecutor(new NamedThreadFactory("ZookeeperServers"));
      service.submit(new ZkServerRunner(properties));
      return service;
    }
  }

  private static class ZkServerRunner implements Callable<Void> {
    private final Properties properties;

    private ZkServerRunner(Properties properties) {
      this.properties = properties;
    }

    @Override
    public Void call() throws Exception {
      final QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
      quorumPeerConfig.parseProperties(properties);
      final QuorumPeerMain quorumPeerMain = new QuorumPeerMain();
      quorumPeerMain.runFromConfig(quorumPeerConfig);
      return null;
    }
  }

  public static class ShutdownHook extends Thread {
    private final Set<ExecutorService> runningServices;

    public ShutdownHook(Set<ExecutorService> runningServices) {
      this.runningServices = runningServices;
    }

    @Override
    public void run() {
      for (ExecutorService service : runningServices) {
        service.shutdown();
        try {
          service.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          LOG.error("shut down service interrupted");
        }
      }
    }
  }

  public static void main(String[] args) {
    LoggingHelper.setLoggingProperties("zookeeper");
    AlertsHandler alertHandler = new InMemoryAlertsHandler();
    String option = args[0];

    Set<ExecutorService> services = Sets.newHashSet();
    try {
      if (option.equals("PRODUCTION")) {
        services = startProductionZk(Integer.valueOf(args[1]));
      } else if (option.equals("TEST")) {
        services = startTestZks();
      } else {
        throw new IllegalArgumentException("No " + option + " available");
      }
    } catch (Exception e) {
      alertHandler.sendAlert("fail to start server", e, AlertRecipients.of("yjin@liveramp.com"));
    } finally {
      Runtime.getRuntime().addShutdownHook(new ShutdownHook(services));
    }
  }
}
