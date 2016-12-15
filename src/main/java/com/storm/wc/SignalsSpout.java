package com.storm.wc;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 指令分发器
 * @author haiswang
 *
 */
public class SignalsSpout implements IRichSpout {
    
    private static final long serialVersionUID = 1L;
    
    private SpoutOutputCollector spoutOutputCollector = null;
    
    @Override
    public void ack(Object arg0) {

    }

    @Override
    public void activate() {

    }

    @Override
    public void close() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void fail(Object arg0) {

    }

    @Override
    public void nextTuple() {
        //发送刷新缓存的指令
        spoutOutputCollector.emit("signals", new Values("reflashCache"));
        
        try {
            //每5s发送一个reflashCache指令
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("signals", new Fields("action"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
