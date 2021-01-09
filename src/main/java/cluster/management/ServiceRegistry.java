package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {
    private static final String REGISTRY_ZNODE = "/service_registry";
    private final ZooKeeper zooKeeper;
    private String currentZNode = null;
    private List<String> allServiceAdresses;

    public ServiceRegistry(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        createServiceRegistryZnode();
    }

    public void registerToCluster(String metadata) throws KeeperException, InterruptedException {
        currentZNode = zooKeeper.create(REGISTRY_ZNODE + "/", metadata.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("Registered to service registry");
    }

    public void registerForUpdates() {
        try {
            updateAdresses();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized List<String> getAllServiceAdresses() throws KeeperException, InterruptedException {
        if (allServiceAdresses == null) {
            updateAdresses();
        }

        return allServiceAdresses;
    }

    public void unregisterFromCluster() {
        try {
            if (currentZNode != null && zooKeeper.exists(currentZNode, false) != null) {
                zooKeeper.delete(currentZNode, -1);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void createServiceRegistryZnode() {
        try {
            if (zooKeeper.exists(REGISTRY_ZNODE, false) == null) { //not exists
                zooKeeper.create(REGISTRY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            System.out.println("multiple creation attempt");
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private synchronized void updateAdresses() throws KeeperException, InterruptedException {
        List<String> workerZNodes = zooKeeper.getChildren(REGISTRY_ZNODE, this);
        ArrayList<String> addresses = new ArrayList<>(workerZNodes.size());

        for (String workerZNode : workerZNodes) {
            String path = REGISTRY_ZNODE + "/" + workerZNode;
            Stat stat = zooKeeper.exists(path, false);
            if (stat == null) {
                continue;
            }

            byte [] addressBytes = zooKeeper.getData(path, false, stat);
            String address = new String(addressBytes);
            addresses.add(address);
        }

        this.allServiceAdresses = Collections.unmodifiableList(addresses);
        System.out.println(allServiceAdresses);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            updateAdresses();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
