package com.storm.wordcount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.joda.time.DateTime;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;



/**
 * spout,数据的采集
 * @author haiswang
 *
 */
public class WCSpout extends BaseRichSpout {
    
    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector = null;
    
    BufferedReader br = null;
    
    @Override
    public void nextTuple() {
        String lineContent = null;
        try {
            lineContent = br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        if(null!=lineContent) {
            System.err.println("线程名 : " + Thread.currentThread().getName() + " " + new DateTime().toString("yyyy-MM-dd HH:mm:ss") + "发射一条数据 : " + lineContent);
            this.collector.emit(new Values(lineContent));
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        InputStream is = WCSpout.class.getClassLoader().getResourceAsStream("wordcount/word.txt");
        br = new BufferedReader(new InputStreamReader(is));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("content"));
    }
}
