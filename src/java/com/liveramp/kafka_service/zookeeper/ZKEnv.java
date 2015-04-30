package com.liveramp.kafka_service.zookeeper;

import java.util.EnumSet;

import com.google.common.base.Joiner;

public class ZKEnv {

  // the number of ticks that the initial synchronization phase can take
  public static final int INIT_LIMIT = 10;
  // the number of ticks that can pass between sending a request and getting an acknowledgement
  public static final int SYNC_LIMIT = 5;

  // Production environments
  public static EnumSet<ZKEnsembles> PRODUCTION_ZKS = EnumSet.of(
      ZKEnsembles.SERVER_0,
      ZKEnsembles.SERVER_1,
      ZKEnsembles.SERVER_2,
      ZKEnsembles.SERVER_3,
      ZKEnsembles.SERVER_4
  );

  // Test environments
  public static EnumSet<ZKEnsembles> TEST_ZKS = EnumSet.of(ZKEnsembles.TEST_SERVER_1, ZKEnsembles.TEST_SERVER_2, ZKEnsembles.TEST_SERVER_3);

  public enum ZKEnsembles {
    //Production Zookeeper servers
    SERVER_0(0, "s2s-data-syner00"),
    SERVER_1(1, "s2s-data-syner01"),
    SERVER_2(2, "s2s-data-syner02"),
    SERVER_3(3, "s2s-data-syner03"),
    SERVER_4(4, "s2s-data-syner04"),

    //Local Zookeeper servers
    TEST_SERVER_1(1, "localhost", 2181, 2888, 3888),
    TEST_SERVER_2(2, "localhost", 2182, 2889, 3889),
    TEST_SERVER_3(3, "localhost", 2183, 2890, 3890);

    public static final int CLIENT_DEFAULT_PORT = 2181;
    public static final int QUORUM_DEFAULT_PORT = 2888;
    public static final int LEADER_DEFAULT_PORT = 3888;

    private final int id;
    private final String host;
    private final int clientPort;
    private final int quorumPort;
    private final int leaderPort;

    ZKEnsembles(int id, String host) {
      this(id, host, CLIENT_DEFAULT_PORT, QUORUM_DEFAULT_PORT, LEADER_DEFAULT_PORT);
    }

    ZKEnsembles(int id, String host, int clientPort, int quorumPort, int leaderPort) {
      this.id = id;
      this.host = host;
      this.clientPort = clientPort;
      this.quorumPort = quorumPort;
      this.leaderPort = leaderPort;
    }

    public int getId() {
      return id;
    }

    public int getClientPort() {
      return clientPort;
    }

    public String getHostPorts() {
      return Joiner.on(":").join(host, quorumPort, leaderPort);
    }
  }

  private ZKEnv() {
    throw new AssertionError("Never be instantiated");
  }
}
