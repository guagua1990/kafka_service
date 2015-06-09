package com.liveramp.kafka_service.zookeeper;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;
import org.I0Itec.zkclient.ZkClient;

public class ZkFs {

  private ZkFs() {
    throw new AssertionError("Can not instantiate ZkFs");
  }

  public static Directory readingCurrentFs(ZkClient zkClient, Directory parent) {
    for (String child : zkClient.getChildren(parent.currentPath.getPath())) {
      Directory childDir = new Directory(new File(parent.currentPath, child));
      parent.childrenPaths.add(childDir);
      readingCurrentFs(zkClient, childDir);
    }
    return parent;
  }

  public static String prettyPrintTree(Directory root) {
    StringBuilder stringBuilder = new StringBuilder();
    prettyPrintTree(stringBuilder, root, new LinkedList<Boolean>());
    return stringBuilder.toString();
  }

  private static void prettyPrintTree(StringBuilder sb, Directory root, LinkedList<Boolean> hasNext) {
    if (root == null) {
      return;
    }

    for (boolean next : hasNext) {
      sb.append(next ? "|" : " ");
      sb.append("  ");
    }
    sb.append("|--");
    sb.append(root.currentPath.getName());
    sb.append("\n");
    hasNext.add(root.childrenPaths.size() > 1);
    for (Directory child : root.childrenPaths) {
      prettyPrintTree(sb, child, hasNext);
    }
    hasNext.removeLast();
  }

  public static class Directory {
    private File currentPath;
    private List<Directory> childrenPaths;

    public Directory(String currentPath) {
      this(new File(currentPath));
    }

    public Directory(File currentPath) {
      this.currentPath = currentPath;
      this.childrenPaths = Lists.newArrayList();
    }

    public boolean isDirectory() {
      return !childrenPaths.isEmpty();
    }
  }
}
