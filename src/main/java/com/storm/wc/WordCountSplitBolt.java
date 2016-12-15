package com.storm.wc;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 分割句子
 * @author haiswang
 *
 */
public class WordCountSplitBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    
    private OutputCollector outputCollector = null;
    
    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
    }

    @Override
    public void execute(Tuple tuple) {
        // TODO Auto-generated method stub
        String line = tuple.getString(0);
        System.err.println("Spilt Bolt Get Line : " + line);
        
        for (String word : line.split(" ")) {
            //向下一个bolt发射单词
            outputCollector.emit(new Values(word));
        }
        
        //对元祖做出应答,确认已成功处理了一个元祖
        outputCollector.ack(tuple);
        
        //这个是对元祖做出处理错误的应答
//        outputCollector.fail(tuple);
        
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        // TODO Auto-generated method stub
        this.outputCollector = outputCollector;
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

}
