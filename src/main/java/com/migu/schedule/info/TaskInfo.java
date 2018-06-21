package com.migu.schedule.info;

/**
 * 任务状态信息类，请勿修改。
 *
 * @author
 * @version
 */
public class TaskInfo
{
    private int taskId;
    private int nodeId;
    private int consumption;
    public int getNodeId()
    {
        return nodeId;
    }
    public int getTaskId(){  return taskId; }
    public void setNodeId(int nodeId)
    {
        this.nodeId = nodeId;
    }
    public void setTaskId(int taskId)
    {
        this.taskId = taskId;
    }

    public int getConsumption() {
        return consumption;
    }

    public void setConsumption(int consumption) {
        this.consumption = consumption;
    }

    public TaskInfo(){
        taskId = 0;
        nodeId = -1;
        consumption = 0;
    }

    public TaskInfo(int id,int con){
        taskId = id;
        consumption = con;
        nodeId = -1;
    }


    @Override
    public String toString()
    {
        return "TaskInfo [taskId=" + taskId + ", nodeId=" + nodeId + "]";
    }
}
