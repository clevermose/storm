package com.storm.wc;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 分割句子,分发单词(已emitDirect的方式替代emit方法),直接数据流
 * @author haiswang
 *
 */
public class WordCountSplitBoltWithDirectEmit implements IRichBolt {

    private static final long serialVersionUID = 1L;
    
    private OutputCollector outputCollector = null;
    
    private List<Integer> nextBoltTasks = null;
    
    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
    }

    @Override
    public void execute(Tuple tuple) {
        // TODO Auto-generated method stub
        String line = tuple.getString(0);
        System.err.println("thread name : " + Thread.currentThread().getName() + ", line value : " + line);
        
        for (String word : line.split(" ")) {
            //向下一个bolt发射单词,使用emitDirect替代emit
            //直接指定下一个bolt的task的index
            
            System.err.println("thread name : " + Thread.currentThread().getName() + ", index : " + getSumBoltIndex(word) + ", word : " + word);
            outputCollector.emitDirect(getSumBoltIndex(word), new Values(word));
        }
        
        //对元祖做出应答,确认已成功处理了一个元祖
        outputCollector.ack(tuple);
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        // TODO Auto-generated method stub
        this.outputCollector = outputCollector;
        //下一个bolt的tasks
        //这边的参数是在WordCountTopology设置的这个bolt的stream的名称
        nextBoltTasks = topologyContext.getComponentTasks("SumBolt");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // TODO Auto-generated method stub
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
    
    /**
     * 通过word的hash值,计算分发下一个bolt的task的index
     * @param word
     * @return
     */
    public int getSumBoltIndex(String word) {
        int index = word.hashCode() % this.nextBoltTasks.size();
        return this.nextBoltTasks.get(index);
    }
}
