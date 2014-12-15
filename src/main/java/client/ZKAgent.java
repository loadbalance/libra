package client;

import common.exception.ConfigException;
import common.exception.ZKOperationFailedException;
import common.util.LibraZKClient;
import common.util.LibraZKPathUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @author xccui
 * Date: 14-10-20
 * Time: 9:26
 */
public class ZKAgent implements Watcher{
    private static Logger LOG = LoggerFactory.getLogger(ZKAgent.class);
    private LibraZKClient zkClient;
    private Object zkTaskLock;
    private Object processLock;
    private String myId;
    private String myAllWorkerPath;
    private LibraClient client;

    public ZKAgent(String myId, String hosts, int sessionTimeout, LibraClient client) throws IOException {
        this.myId = myId;
        zkClient = new LibraZKClient(hosts, sessionTimeout, this);
        this.client = client;
        zkTaskLock = new Object();
        processLock = new Object();
        myAllWorkerPath  = LibraZKPathUtil.genMyAllWorkerPath(myId);
    }

    /**
     * Register client with myId to ZK
     *
     * @throws KeeperException
     * @throws InterruptedException
     * @throws ConfigException
     */
    void register() throws KeeperException, InterruptedException, ConfigException {
        zkClient.checkAndCreateNode(LibraZKPathUtil.ALL_WORKER_ROOT, null, CreateMode.PERSISTENT);
        if (zkClient.checkAndCreateNode(myAllWorkerPath, new byte[0], CreateMode.EPHEMERAL)) {//myId node already exists
            throw new ConfigException("id node " + myId + " already exists at zookeeper");
        }
        //add watcher to my node
        zkClient.getDataString(myAllWorkerPath, null);
    }

    List<String> fetchWorkerList(String currentWorkerRoot) throws KeeperException, InterruptedException {
            zkClient.checkAndCreateNode(currentWorkerRoot, new byte[0], CreateMode.PERSISTENT);
            return Collections.unmodifiableList(zkClient.getChildren(currentWorkerRoot));
    }

    List<String> fetchTaskList(String currentTaskRoot) throws KeeperException, InterruptedException {
            zkClient.checkAndCreateNode(currentTaskRoot, new byte[0], CreateMode.PERSISTENT);
            return Collections.unmodifiableList(zkClient.getChildren(currentTaskRoot));
    }

    String fetchMyProjectName() throws KeeperException, InterruptedException {
            return zkClient.getDataString(myAllWorkerPath, null);
    }


    void checkAndOwnTasks(String projectName, Set<String> myTasks, List<String> workerList, Set<String> ownedTasks) throws KeeperException, InterruptedException, ZKOperationFailedException {
        synchronized (zkTaskLock) {
            if (!myTasks.isEmpty()) {
                String taskOwner;
                Set<String> workerSet = new HashSet<>(workerList);
                for(String myTask : myTasks){
                    taskOwner = zkClient.checkAndGetDataString(LibraZKPathUtil.genSingleTaskPath(myTask, projectName));
                    if (taskOwner.length() > 0 && workerSet.contains(taskOwner) && !taskOwner.equals(myId)) {
                        //not released yet
                    } else {
                        //can be owned
                        zkClient.compareAndUpdateData(LibraZKPathUtil.genSingleTaskPath(myTask, projectName), taskOwner, myId.getBytes(), false);
                        ownedTasks.add(myTask);
                    }
                }
                LOG.info("Owned tasks: " + ownedTasks);
                if( myTasks.size() != ownedTasks.size()) {
                    myTasks.removeAll(ownedTasks);
                    throw new ZKOperationFailedException("Task " + myTasks + " are not owned");
                }
            }
        }
    }

    /**
     * Release my owned tasks from ZK
     *
     * @param projectName
     * @param taskToBeReleased
     */
    void releaseTasks(String projectName, Set<String> taskToBeReleased, Set<String> taskReleased) throws KeeperException, InterruptedException, ZKOperationFailedException {
        synchronized (zkTaskLock) {
            if (!taskToBeReleased.isEmpty()) {
                LinkedList<String> ownedTasks = new LinkedList<>(taskToBeReleased);
                for(String task : taskToBeReleased){
                    task = ownedTasks.removeFirst();
                    if(zkClient.compareAndUpdateData(LibraZKPathUtil.genSingleTaskPath(task, projectName), myId, new byte[0], false)) {
                        taskReleased.add(task);
                    }
                }
                LOG.info("Released tasks: " + taskReleased);
                if(taskReleased.size() != taskReleased.size()) {
                    taskToBeReleased.removeAll(taskReleased);
                    throw new ZKOperationFailedException("Task " + taskToBeReleased + " are not released");
                }
            }
        }
    }

    void addMyIdToActiveWorkerRoot(String newProjectName) throws KeeperException, InterruptedException {
        zkClient.checkAndCreateNode(LibraZKPathUtil.genMyActiveWorkerPath(myId, newProjectName), new byte[0], CreateMode.EPHEMERAL);
        zkClient.checkAndCreateNode(LibraZKPathUtil.genTaskRootPath(newProjectName), new byte[0], CreateMode.PERSISTENT);
        zkClient.checkAndCreateNode(LibraZKPathUtil.genActiveWorkerRootPath(newProjectName), new byte[0], CreateMode.PERSISTENT);
    }

    void deleteMyIdFromActiveWorkerRoot(String oldProjectName) throws KeeperException, InterruptedException {
        zkClient.checkAndDeleteNode(LibraZKPathUtil.genMyActiveWorkerPath(myId, oldProjectName));
    }

    void resetUnsupportedProjectName(String newProjectName, String oldProjectName) throws KeeperException, InterruptedException {
        zkClient.compareAndUpdateData(myAllWorkerPath, newProjectName, oldProjectName.getBytes(), false);
    }


    public void close() throws InterruptedException {
        if (null != zkClient) {
            zkClient.close();
        }
    }

    public void reset() {
        zkClient.reconnect();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        synchronized (processLock) {
            LOG.info("zkWatcher process:" + watchedEvent.getState() + "\t" + watchedEvent.getPath() + "\t" + watchedEvent.getType());
            if (watchedEvent.getState().equals(Event.KeeperState.Disconnected)) {
                LOG.warn("Zookeeper disconnected! Will try to reconnect!");
                client.restart();
            } else {
                try{
                    if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                        if (watchedEvent.getPath().contains(client.projectState.getCurrentWorkerRoot())) {
                            // current worker list changed
                            client.handleEvent(LibraClient.LibraClientEvent.workerListChanged);
                        } else if (watchedEvent.getPath().contains(client.projectState.getCurrentTaskRoot())) {
                            // current task list changed
                            client.handleEvent(LibraClient.LibraClientEvent.taskListChanged);
                        }
                    } else if (watchedEvent.getType() == Event.EventType.NodeDataChanged && watchedEvent.getPath().contains(LibraZKPathUtil.ALL_WORKER_ROOT)) {
                        // my project was changed
                        client.handleEvent(LibraClient.LibraClientEvent.projectChanged);
                    } else if (watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().contains(LibraZKPathUtil.ALL_WORKER_ROOT)) {
                        // this client was removed by server
                        client.forceExit();
                    }
                }catch(InterruptedException ex){
                    ex.printStackTrace();
                    LOG.warn("Process interrupted!");
                }
            }
        }
    }
}
