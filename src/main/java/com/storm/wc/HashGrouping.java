package com.storm.wc;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

/**
 * 自定义Hash分组
 * @author haiswang
 *
 */
public class HashGrouping implements CustomStreamGrouping, Serializable {
    
    private static final long serialVersionUID = 1L;
    
    //bolt中执行任务的个数
    private List<Integer> targetTasks = null;
    
    @Override
    public List<Integer> chooseTasks(int arg0, List<Object> values) {
        String value = values.get(0).toString();
        int index = value.hashCode() % this.targetTasks.size();
        return Arrays.asList(targetTasks.get(index));
    }

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId,
            List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
    }

}
