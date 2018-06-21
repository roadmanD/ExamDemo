package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.*;

/*
*类名和方法不能修改
 */
public class Schedule {
    //服务器注册节点
    private static List<Integer> nodeList;
    //服务器注册节点资源消耗率
    private Map<Integer,Integer> nodeConsumptionMap;
    //挂起队列
    private List<TaskInfo> waitTaskInfoList;
    //服务器执行任务队列
    private Map<Integer,List<TaskInfo>> nodeTaskMap;
    //任务信息
    private Map<Integer,TaskInfo> taskInfoMap;


    private static Map<Integer,List<TaskInfo>> nodeTaskTestMap;

    /**
     * 2.1	系统初始化
     * @return
     */
    public int init() {
        nodeList = new ArrayList<Integer>();
        nodeConsumptionMap = new HashMap<Integer, Integer>();
        waitTaskInfoList = new ArrayList<TaskInfo>();
        nodeTaskMap = new HashMap<Integer, List<TaskInfo>>();
        taskInfoMap = new HashMap<Integer, TaskInfo>();
        nodeTaskTestMap = new HashMap<Integer, List<TaskInfo>>();
        return ReturnCodeKeys.E001;
    }

    /**
     * 2.2	服务节点注册
     * @param nodeId
     * @return
     */
    public int registerNode(int nodeId) {
        //如果服务节点编号小于等于0, 返回E004:服务节点编号非法
        if(nodeId <= 0){
            return ReturnCodeKeys.E004;
        }
        //如果服务节点编号已注册, 返回E005:服务节点已注册
        if(nodeConsumptionMap.containsKey(nodeId)){
            return ReturnCodeKeys.E005;
        }

        nodeList.add(nodeId);
        nodeConsumptionMap.put(nodeId,0);
        //注册成功，返回E003:服务节点注册成功
        return ReturnCodeKeys.E003;
    }

    /**
     * 2.3	服务节点注销
     * @param nodeId
     * @return
     */
    public int unregisterNode(int nodeId) {
        //如果服务节点编号小于等于0, 返回E004:服务节点编号非法
        if(nodeId <= 0){
            return ReturnCodeKeys.E004;
        }
        //如果服务节点编号未被注册, 返回E007:服务节点不存在
        if(!nodeConsumptionMap.containsKey(nodeId)){
            return ReturnCodeKeys.E007;
        }
        for(int i = 0; i < nodeList.size(); i++){
            if(nodeList.get(i) == nodeId){
                nodeList.remove(i);
                break;
            }
        }

        nodeConsumptionMap.remove(nodeId);
        if(nodeTaskMap.containsKey(nodeId)){
            List<TaskInfo> taskInfoList = nodeTaskMap.get(nodeId);
            if(taskInfoList != null && taskInfoList.size() > 0){
                taskInfoList.addAll(taskInfoList);
            }
        }
        nodeTaskMap.remove(nodeId);
        //注销成功，返回E006:服务节点注销成功
        return ReturnCodeKeys.E006;
    }

    /**
     * 2.4	添加任务
     * @param taskId
     * @param consumption
     * @return
     */
    public int addTask(int taskId, int consumption) {
        //如果任务编号小于等于0, 返回E009:任务编号非法
        if(taskId <= 0){
            return ReturnCodeKeys.E009;
        }
        //如果相同任务编号任务已经被添加, 返回E010:任务已添加
        if(taskInfoMap.containsKey(taskId)){
            return ReturnCodeKeys.E010;
        }

        TaskInfo taskInfo = new TaskInfo(taskId, consumption);
        waitTaskInfoList.add(taskInfo);
        taskInfoMap.put(taskId, taskInfo);
        //添加成功，返回E008任务添加成功
        return ReturnCodeKeys.E008;
    }

    /**
     * 2.5	删除任务
     * 将在挂起队列中的任务 或 运行在服务节点上的任务删除
     * @param taskId
     * @return
     */
    public int deleteTask(int taskId) {
        //如果任务编号小于等于0, 返回E009:任务编号非法
        if(taskId <= 0){
            return ReturnCodeKeys.E009;
        }
        //如果指定编号的任务未被添加, 返回E012:任务不存在
        if(!taskInfoMap.containsKey(taskId)){
            return ReturnCodeKeys.E012;
        }
        TaskInfo taskInfo = taskInfoMap.get(taskId);
        //如果节点为空，则在挂起队列
        if(taskInfo.getNodeId() == -1){
            for(int i = 0; i < waitTaskInfoList.size(); i++ ){
                TaskInfo task = waitTaskInfoList.get(i);
                if(task.getTaskId() == taskId){
                    waitTaskInfoList.remove(i);
                    break;
                }
            }
        } else {
            int nodeId = taskInfo.getNodeId();
            List<TaskInfo> tiList = nodeTaskMap.get(nodeId);
            for(int j = 0; j < tiList.size(); j++ ){
                if(tiList.get(j).getTaskId() == taskId){
                    tiList.remove(j);
                    break;
                }
            }
            nodeTaskMap.put(nodeId,tiList);
        }
        taskInfoMap.remove(taskId);

        //删除成功，返回E011:任务删除成功
        return ReturnCodeKeys.E011;
    }

    /**
     * 2.6	任务调度
     * @param threshold
     * @return
     */
    public int scheduleTask(int threshold) {
        //如果调度阈值取值错误，返回E002调度阈值非法
        if(threshold <= 0){
            return ReturnCodeKeys.E002;
        }
        List<TaskInfo> sumTaskInfoList = getSumTaskInfoList();
        if(sumTaskInfoList.size() <= nodeList.size()){
            int cut = 0;
            for(int i = 0; i < sumTaskInfoList.size() - 1; i++){
                int com = Math.abs(sumTaskInfoList.get(i).getConsumption() - sumTaskInfoList.get(i++).getConsumption());
                if(cut < com){
                    cut = com;
                }
                if(cut > threshold){
                    return ReturnCodeKeys.E014;
                }
            }
            //可分配
            waitTaskInfoList = new ArrayList<TaskInfo>();

            for(int i = 0; i < sumTaskInfoList.size() - 1; i++){
                TaskInfo taskInfo = sumTaskInfoList.get(i);
                taskInfo.setNodeId(nodeList.get(i));
                List<TaskInfo> tiList = new ArrayList<TaskInfo>();
                tiList.add(taskInfo);
                nodeTaskMap.put(nodeList.get(i),tiList);
                taskInfoMap.put(taskInfo.getTaskId(),taskInfo);
            }
        } else {
            //把账单按从大到小排序
            Collections.sort(sumTaskInfoList,new Comparator<TaskInfo>() {
                @Override
                public int compare(TaskInfo o1, TaskInfo o2) {
                    if(o1.getConsumption() < o2.getConsumption()){
                        return -1;
                    }else if(o1.getConsumption() > o2.getConsumption()){
                        return 1;
                    }
                    return 0;
                }
            });

            for(int i = 0; i < sumTaskInfoList.size() - 1; i++){
                if(Math.abs(sumTaskInfoList.get(i).getConsumption() - sumTaskInfoList.get(i+1).getConsumption()) > threshold){
                    return ReturnCodeKeys.E014;
                }
            }


            for(int i = 0; i < nodeList.size(); i++){
                ArrayList<TaskInfo> tList = new ArrayList<TaskInfo>();
                TaskInfo t = sumTaskInfoList.get(sumTaskInfoList.size() - i - 1);
                t.setNodeId(nodeList.get(i));
                tList.add(t);

                taskInfoMap.put(t.getNodeId(),t);
                nodeTaskTestMap.put(nodeList.get(i),tList);
                nodeConsumptionMap.put(nodeList.get(i),tList.get(0).getConsumption());
            }

            for(int i = sumTaskInfoList.size() - 1;i >  nodeList.size() - 1; i--){
                Integer nodeId = getMixComNodeId();
                List<TaskInfo> tList = nodeTaskTestMap.get(nodeId);
                TaskInfo tt = sumTaskInfoList.get(i);
                tt.setNodeId(nodeId);
                tList.add(tt);
                taskInfoMap.put(tt.getNodeId(),tt);
                nodeTaskTestMap.put(nodeId,tList);
                Integer com = 0;
                for(TaskInfo t : tList){
                    com += t.getConsumption();
                }
                nodeConsumptionMap.put(nodeId,com);
            }

            Integer mixConNodeId = getMixComNodeId();

            Integer maxConNodeId = getMaxComNodeId();

            if(nodeConsumptionMap.get(maxConNodeId) - nodeConsumptionMap.get(mixConNodeId) > threshold){
                return ReturnCodeKeys.E014;
            }

            nodeTaskMap = nodeTaskTestMap;
        }

        return ReturnCodeKeys.E013;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        //如果查询结果参数tasks为null，返回E016:参数列表非法
        if(tasks == null){
            return ReturnCodeKeys.E016;
        }
        List<TaskInfo> taskList = getSumTaskInfoList();

        Collections.sort(taskList,new Comparator<TaskInfo>() {
            @Override
            public int compare(TaskInfo o1, TaskInfo o2) {
                if(o1.getTaskId() < o2.getTaskId()){
                    return -1;
                }else if(o1.getTaskId() > o2.getTaskId()){
                    return 1;
                }
                return 0;
            }
        });

        tasks = taskList;

        return ReturnCodeKeys.E015;
    }


    public List<TaskInfo> getSumTaskInfoList(){
        List<TaskInfo> taskList = new ArrayList<TaskInfo>();
        for(Integer key : taskInfoMap.keySet()){
            taskList.add(taskInfoMap.get(key));
        }
        return taskList;
    }



    public Integer getMixComNodeId(){
        Integer com = 0;
        Integer nodeId = 0;
        for(Integer key : nodeConsumptionMap.keySet()){
            if(com == 0){
                com = nodeConsumptionMap.get(key);
                nodeId = key;
            }
            if(nodeConsumptionMap.get(key) < com){
                com = nodeConsumptionMap.get(key);
                nodeId = key;
            }
        }
        return nodeId;
    }

    public Integer getMaxComNodeId(){
        Integer com = 0;
        Integer nodeId = 0;
        for(Integer key : nodeConsumptionMap.keySet()){
            if(com == 0){
                com = nodeConsumptionMap.get(key);
                nodeId = key;
            }
            if(nodeConsumptionMap.get(key) > com){
                com = nodeConsumptionMap.get(key);
                nodeId = key;
            }
        }
        return nodeId;
    }

}
