package client.state;

import common.exception.OperationOutOfDateException;

/**
 * @author xccui
 *         Date: 14-10-20
 *         Time: 11:15
 */
public class ZKDataVersion {
    private long projectVersion;
    private long taskListVersion;
    private long workerListVersion;

    public synchronized void updateProjectVersion() {
        projectVersion++;
    }

    public synchronized long getProjectVersion() {
        return projectVersion;
    }

    public synchronized long getWorkerListVersion() {
        return workerListVersion;
    }

    public synchronized long getTaskListVersion() {
        return taskListVersion;
    }

    public synchronized void updateTaskListVersion() {
        taskListVersion++;

    }

    public synchronized void updateWorkerListVersion() {
        workerListVersion++;
    }

    public synchronized ZKDataVersion makeSnapshot() {
        ZKDataVersion snapshot = new ZKDataVersion();
        snapshot.projectVersion = projectVersion;
        snapshot.workerListVersion = workerListVersion;
        snapshot.taskListVersion = taskListVersion;
        return snapshot;
    }

    public String getStateString() {
        return projectVersion + " " + taskListVersion + " " + workerListVersion;
    }

    public synchronized void checkOutOfDate(ZKDataVersion currentDataVersion) throws OperationOutOfDateException {
        if (null == currentDataVersion) {
            return;
        }
        if (!(projectVersion == currentDataVersion.projectVersion && workerListVersion == currentDataVersion.workerListVersion
                && taskListVersion == currentDataVersion.taskListVersion)) {
            throw new OperationOutOfDateException("State is out of date - " + getStateString() + " | " + currentDataVersion.getStateString());
        }
    }
}
