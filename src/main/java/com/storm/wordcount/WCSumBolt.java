package com.storm.wordcount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * storm,数据的统计
 * @author haiswang
 *
 */
public class WCSumBolt extends BaseRichBolt {
    
    private static final long serialVersionUID = 1L;
    
    private Map<String, Integer> wordCount = new HashMap<String, Integer>();
    
    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        
        if(wordCount.containsKey(word)) {
            wordCount.put(word, wordCount.get(word) + 1);
        } else {
            wordCount.put(word, 1);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        new Thread(new Runnable() {
            @SuppressWarnings("static-access")
            @Override
            public void run() {
                while(true) {
                    System.err.println("statics start.......................");
                    for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
                        System.err.println(entry.getKey() + " : " + entry.getValue());
                    }
                    System.err.println("statics end.........................");
                    
                    try {
                        Thread.currentThread().sleep(3000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        
    }
}
