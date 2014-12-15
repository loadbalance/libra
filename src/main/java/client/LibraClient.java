package client;

import client.config.LibraClientConfig;
import client.state.ZKDataVersion;
import client.state.ProjectState;
import common.exception.*;
import common.util.LibraZKPathUtil;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


/**
 * Main class for libra client
 *
 * @author xccui
 *         Date: 13-9-16
 *         Time: 10:09
 */
public class LibraClient {
    private static Logger LOG = LoggerFactory.getLogger(LibraClient.class);

    String myId;
    String myAllWorkerPath;
    String hosts;
    int sessionTimeout;
    final int retryInterval = LibraClientConfig.getIntProperty(LibraClientConfig.RETRY_INTERVAL_KEY);

    public enum LibraClientEvent {
        projectChanged, taskListChanged, workerListChanged
    }

    private ExecutorFactory executorFactory;

    ProjectState projectState;
    private List<String> workerList;
    private List<String> taskList;
    private ZKDataVersion currentState;

    private Object rebalanceLock;
    private IRebalanceTool rebalanceTool;
    private ZKAgent agent;

    public LibraClient(String myId, String hosts, int sessionTimeout, ExecutorFactory executorFactory) throws IOException {
        this.myId = myId;
        this.hosts = hosts;
        this.sessionTimeout = sessionTimeout;
        this.executorFactory = executorFactory;
        myAllWorkerPath = LibraZKPathUtil.genMyAllWorkerPath(myId);
        agent = new ZKAgent(myId, hosts, sessionTimeout, this);
        rebalanceTool = new DefaultRebalanceTool();
        rebalanceLock = new Object();
        currentState = new ZKDataVersion();
    }

    void register() throws InterruptedException, ConfigException, KeeperException {
        agent.register();
        //init project stat
        projectState = ProjectState.createStandbyState();
    }

    public void start() throws KeeperException, InterruptedException, ConfigException, IOException {
        workerList = new ArrayList<>();
        taskList = new ArrayList<>();
        register();
    }

    public void restart() {
        try {
            agent.close();
            executorFactory.stopCurrentExecutor();
        } catch (ExecutorException | InterruptedException e) {
            e.printStackTrace();
        }
        try {
            start();
        } catch (Exception ex) {
            ex.printStackTrace();
            forceExit();
        }
    }

    private synchronized void activate(final String projectName) throws KeeperException, InterruptedException, ExecutorException {
        LOG.info("Activate - projectName:" + projectName);
        agent.addMyIdToActiveWorkerRoot(projectName);
        executorFactory.loadExecutor(projectName);
        currentState.updateProjectVersion();
    }

    private synchronized void inactivate(final String oldProjectName) throws KeeperException, InterruptedException, WrongStateException {
        LOG.info("Inactivate - projectName:" + oldProjectName);
        executorFactory.stopCurrentExecutor();
        agent.deleteMyIdFromActiveWorkerRoot(oldProjectName);
        currentState.updateProjectVersion();
    }

    private synchronized void reassignTasks(String projectName, List<String> taskList, Set<String> taskToBeReleased, Set<String> taskReleased, Set<String> taskTobeOwned, Set<String> taskOwned) throws OperationOutOfDateException, ExecutorException, KeeperException, InterruptedException, ZKOperationFailedException {
        taskToBeReleased.removeAll(taskReleased);
        taskTobeOwned.removeAll(taskOwned);
        executorFactory.pauseCurrentExecutor(projectName);
        agent.releaseTasks(projectName, taskToBeReleased, taskReleased);
        agent.checkAndOwnTasks(projectName, taskTobeOwned, workerList, taskOwned);
        executorFactory.startCurrentExecutor(projectName, taskList);
    }

    private ZKDataVersion updateZKData(LibraClientEvent event) throws KeeperException, InterruptedException, ExecutorException {
        switch (event) {
            case projectChanged:
                String newProjectName = agent.fetchMyProjectName();
                ProjectState oldProjectState = projectState;
                if (oldProjectState.isNewProjectName(newProjectName) && newProjectName.length() > 0) {
                    projectState = ProjectState.createActiveState(newProjectName);
                    if (executorFactory.isProjectSupported(newProjectName)) {
                        if (oldProjectState.isActive()) {
                            //reactive
                            LOG.info("From active to active");
                            inactivate(oldProjectState.getProjectName());
                        }
                        //active
                        LOG.info("From inactive to active");
                        activate(newProjectName);
                        setWorkerList(agent.fetchWorkerList(projectState.getCurrentWorkerRoot()));
                        LOG.debug("Update workers: " + getWorkerList().toString());
                        setTaskList(agent.fetchTaskList(projectState.getCurrentTaskRoot()));
                        LOG.debug("Update tasks: " + getTaskList().toString());
                    } else {
                        LOG.info("Not supported projectName " + newProjectName + ", will reset to " + oldProjectState.getProjectName());
                        projectState = oldProjectState;
                        agent.resetUnsupportedProjectName(newProjectName, oldProjectState.getProjectName());
                    }
                } else if (newProjectName.length() == 0) {
                    if (oldProjectState.isActive()) {
                        //inactive
                        LOG.info("from active to inactive");
                        inactivate(oldProjectState.getProjectName());
                        projectState = ProjectState.createStandbyState();
                    }
                }
                break;
            case workerListChanged:
                setWorkerList(agent.fetchWorkerList(projectState.getCurrentWorkerRoot()));
                LOG.debug("Update workers: " + getWorkerList().toString());
                break;
            case taskListChanged:
                setTaskList(agent.fetchTaskList(projectState.getCurrentTaskRoot()));
                LOG.debug("Update tasks: " + getTaskList().toString());
                break;
            default:
                System.err.println("Unknown event: " + event);
                System.exit(1);
        }
        return currentState.makeSnapshot();
    }

    void handleEvent(LibraClientEvent event) throws InterruptedException {
        LOG.info(event.name() + " start");
        int retry = LibraClientConfig.getIntProperty(LibraClientConfig.RETRY_TIMES_KEY);
        boolean success = false;
        ZKDataVersion snapshot = null;
        while (!success && retry >= 0) {
            try {
                snapshot = updateZKData(event);
                success = true;
            } catch (Exception e) {
                if (e instanceof KeeperException) {
                    agent.reset();
                }
                e.printStackTrace();
                LOG.warn("Update ZooKeeper data failed, " + retry + "times left");
                --retry;
            }
        }
        if (retry < 0) {
            forceExit();
        }
        TransactionThread transaction = new TransactionThread(snapshot);
        transaction.calculateMyTasks(projectState.getProjectName(), workerList, taskList);
        transaction.start();
    }


    void forceExit() {
        LOG.error("Fatal error! Will exit to release resources.");
        try {
            executorFactory.stopCurrentExecutor();
            agent.close();
        } catch (ExecutorException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.exit(-1);
        }
    }

    void setTaskList(List<String> newCurrentTaskList) {
        this.taskList = newCurrentTaskList;
        currentState.updateTaskListVersion();
    }

    void setWorkerList(List<String> newCurrentWorkerList) {
        this.workerList = newCurrentWorkerList;
        currentState.updateWorkerListVersion();
    }

    List<String> getTaskList() {
        return taskList;
    }

    List<String> getWorkerList() {
        return workerList;
    }

    static void showUsage() {
        System.out.println("Usage: -n worker_name [-t timeout(default:10000)]|[-h host (default:localhost)]");
    }

    public static void main(String args[]) {
        try {
            String workerName = "worker1";
            String host = "211.87.224.213:2181";
            int timeout = 10000;
            if (args.length == 2) {
                if ("-n".equals(args[0])) {
                    workerName = args[1];
                } else {
                    showUsage();
                }
            } else if (args.length == 4) {
                if ("-n".equals(args[0]) && "-t".equals(args[2])) {
                    workerName = args[1];
                    timeout = Integer.valueOf(args[3]);
                } else if ("-n".equals(args[0]) && "-h".equals(args[2])) {
                    workerName = args[1];
                    host = args[3];
                } else {
                    showUsage();
                    System.exit(1);
                }
            } else if (args.length == 6) {
                if ("-n".equals(args[0]) && "-t".equals(args[2]) && "-h".equals(args[4])) {
                    workerName = args[1];
                    timeout = Integer.valueOf(args[3]);
                    host = args[5];
                } else {
                    showUsage();
                    System.exit(1);
                }

            } else {
                showUsage();
                System.exit(1);
            }
            ExecutorFactory factory = new ExecutorFactory();
//                factory.loadExecutorsFromFile("executors.xml");
            LibraClient client = new LibraClient(workerName, host, timeout, new ExecutorFactory());
            client.start();
            System.out.println("INFO: Worker " + workerName + " at " + host + " is Running... ");
            Thread.sleep(6000000);
            System.out.println("INFO: Worker " + workerName + "exit.");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ConfigException e) {
            e.printStackTrace();
        }
    }

    private class TransactionThread extends Thread {
        String projectName;
        ZKDataVersion snapshot;
        List<String> myTaskList;

        public TransactionThread(ZKDataVersion snapshot) {
            this.snapshot = snapshot;
        }

        public void calculateMyTasks(String projectName, List<String> workerList, List<String> taskList) {
            this.projectName = projectName;
            myTaskList = rebalanceTool.calculateMyTask(myId, taskList, workerList);
            LOG.info("\nprojectName = " + projectName + "\nworkerList = " + workerList + "\ntaskList = " + taskList + "\nmyTaskList = " + myTaskList);
        }

        @Override
        public void run() {
            synchronized (rebalanceLock) {
                super.run();
                int retry = LibraClientConfig.getIntProperty(LibraClientConfig.RETRY_TIMES_KEY);
                Set<String> taskToBeReleased = new HashSet<>(executorFactory.getCurrentTaskList());
                Set<String> taskToBeOwned = new HashSet<>(myTaskList);
                Set<String> taskReleased = new HashSet<>();
                Set<String> taskOwned = new HashSet<>();
                boolean success = false;
                LOG.info("=============== Handle Event Thread Start ===============");
                while (!success && retry > 0) {
                    try {
                        currentState.checkOutOfDate(snapshot);
                        if (!projectState.isActive()) {
                            throw new ZKOperationFailedException("Project state is not active.");
                        }
                        reassignTasks(projectName, myTaskList, taskToBeReleased, taskReleased, taskToBeOwned, taskOwned);
                        success = true;
                        break;
                    } catch (OperationOutOfDateException ex) {
                        ex.printStackTrace();
                        LOG.warn("Operation is out of date! Will rollback!");
                        Set<String> rollbackTaskReleased = new HashSet<>();
                        retry = LibraClientConfig.getIntProperty(LibraClientConfig.RETRY_TIMES_KEY);
                        success = false;
                        while (!success && retry >= 0) {
                            try {
                                agent.releaseTasks(projectState.getProjectName(), taskOwned, rollbackTaskReleased);
                                success = true;
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                                forceExit();
                            } catch (Exception e) {
                                e.printStackTrace();
                                if (e instanceof KeeperException) {
                                    agent.reset();
                                }
                                --retry;
                                LOG.warn("Exception encountered! Will retry in " + retryInterval + " ms, " + retry + " times left.");
                                try {
                                    Thread.sleep(retryInterval);
                                } catch (InterruptedException exc) {
                                    exc.printStackTrace();
                                }
                            }
                        }
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                        forceExit();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        if (ex instanceof KeeperException) {
                            agent.reset();
                        }
                        --retry;
                        LOG.warn("Exception encountered! Will retry in " + retryInterval + " ms, " + retry + " times left.");
                        try {
                            Thread.sleep(retryInterval);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } finally {
                        if (retry <= 0) {
                            forceExit();
                        }
                    }
                }
                LOG.info("=============== Handle Event Thread End ===============");
            }
        }
    }
}
