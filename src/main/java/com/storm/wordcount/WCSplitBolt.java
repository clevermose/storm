package com.storm.wordcount;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * storm,数据的切分
 * @author haiswang
 *
 */
public class WCSplitBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    
    private OutputCollector outputCollector = null;
    
    @Override
    public void execute(Tuple tuple) {
        String lineContent = tuple.getString(0);
        System.err.println("thread name : " + Thread.currentThread().getName() + ", Get line content : " + lineContent);
        
        for (String word : lineContent.split(" ")) {
            outputCollector.emit(new Values(word));
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
