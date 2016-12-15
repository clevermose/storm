package com.storm.wc;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * 统计单词
 * @author haiswang
 *
 */
public class WordCountSumBolt implements IRichBolt {
    
    private static final long serialVersionUID = 1L;

    private Map<String, Integer> wordCount = null;
    
    private OutputCollector outputCollector = null;
    
    private Integer id;
    
    private String name;
    
    /**
     * 拓扑关闭(集群关闭)的时候会调用该方法
     * 一般在该方法中关闭资源信息
     */
    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
        System.err.println("----单词数[" + name + " ," + id + "]----");
        for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
            System.err.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    @Override
    public void execute(Tuple tuple) {
        //指令
        String command = null;
        
        //判断是否是刷新缓存指令
        if("signals".equals(tuple.getSourceStreamId())) {
            command = tuple.getStringByField("action");
            if("reflashCache".equals(command)) {
                wordCount.clear();
                return;
            }
        }
        
        String word = tuple.getString(0);
        
        if(this.wordCount.containsKey(word)) {
            int wordCnt = this.wordCount.get(word);
            this.wordCount.put(word, wordCnt + 1);
        } else {
            this.wordCount.put(word, 1);
        }
        
        //确认元祖已成功处理
        this.outputCollector.ack(tuple);
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        // TODO Auto-generated method stub
        this.wordCount = new HashMap<String, Integer>();
        this.outputCollector = outputCollector;
        
        this.id = topologyContext.getThisTaskId();
        this.name = topologyContext.getThisComponentId();
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
