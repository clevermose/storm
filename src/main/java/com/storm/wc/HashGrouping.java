package com.storm.wc;

import java.io.Serializable;
import java.util.ArrayList;
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
    private int taskNums = 0;
    
    @Override
    public List<Integer> chooseTasks(int arg0, List<Object> values) {
        
        ArrayList<Integer> boltIds = new ArrayList<>(values.size());
        
        for (Object value : values) {
            //依据value的hash值,进行分发
            int index = value.hashCode() % taskNums;
            boltIds.add(index);
        }
        
        return boltIds;
    }

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId,
            List<Integer> targetTasks) {
        taskNums = targetTasks.size();
    }

}
