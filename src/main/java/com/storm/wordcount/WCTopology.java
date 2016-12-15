package com.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


public class WCTopology {
    
    public static void main(String[] args) throws InterruptedException {
        
        TopologyBuilder builder=new TopologyBuilder();  
        builder.setSpout("ReadSpout", new WCSpout());
        builder.setBolt("SplitBolt",new WCSplitBolt()).shuffleGrouping("ReadSpout");
        builder.setBolt("SumBolt", new WCSumBolt()).shuffleGrouping("SplitBolt");
        
        Config config = new Config();
        config.setDebug(false);
        
        config.setMaxTaskParallelism(1);;  
        LocalCluster cluster=new LocalCluster();  
        cluster.submitTopology("word-count",config,builder.createTopology());  
        Thread.sleep(30000);  
        cluster.shutdown(); 
    }
    
}
