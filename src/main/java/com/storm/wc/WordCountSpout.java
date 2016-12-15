package com.storm.wc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 读取文件
 * @author haiswang
 *
 */
public class WordCountSpout implements IRichSpout {

    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector = null;
    
    private FileReader fileReader = null;
    
    //是否完成
    private boolean isComplate = false;
    
    @Override
    public void ack(Object msgId) {
        System.err.println("OK : " + msgId);
    }

    @Override
    public void activate() {
        // TODO Auto-generated method stub
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub
    }

    @Override
    public void fail(Object msgId) {
        System.err.println("FAIL : " + msgId);
    }

    @Override
    public void nextTuple() {
        if(isComplate) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
            }
            
            return;
        }
        
        BufferedReader br = new BufferedReader(fileReader);
        String line = null;
        try {
            while(null!=(line=br.readLine())) {
                collector.emit(new Values(line), line);
            }
        } catch (IOException e) {
            throw new RuntimeException("read tuple failed.", e);
        } finally {
            isComplate = true;
        }
    }
    
    /**
     * Map config : 配置信息,对应Topology中的config
     */
    @Override
    public void open(@SuppressWarnings("rawtypes") Map config, TopologyContext topologyContext, SpoutOutputCollector collector) {
        
        try {
            fileReader = new FileReader(config.get("filePath").toString());
        } catch (FileNotFoundException e) {
            new RuntimeException("read file failed. file path : " + config.get("filePath"));
        }
        
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // TODO Auto-generated method stub
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
