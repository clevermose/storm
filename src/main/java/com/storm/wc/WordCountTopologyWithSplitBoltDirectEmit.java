package com.storm.wc;

import java.util.concurrent.TimeUnit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class WordCountTopologyWithSplitBoltDirectEmit {

    /**
     * @param args
     */
    public static void main(String[] args) {
        
        TopologyBuilder builder=new TopologyBuilder();
        
        builder.setSpout("ReadSpout", new WordCountSpout());
        builder.setBolt("SplitBolt",new WordCountSplitBoltWithDirectEmit()).shuffleGrouping("ReadSpout");
        //由于SplitBolt中的emit是emitDirect,所以这边直接使用directGrouping
        builder.setBolt("SumBolt", new WordCountSumBolt()).directGrouping("SplitBolt");
                
        Config config = new Config();
        config.setDebug(false);
        config.put("filePath", "e:\\word.txt");
        
        //config.setMaxTaskParallelism(1);;  
        LocalCluster cluster=new LocalCluster();  
        cluster.submitTopology("word-count",config,builder.createTopology());  
        
        try {
            TimeUnit.SECONDS.sleep(20);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        //关闭集群
        cluster.shutdown(); 
        
    }

}
