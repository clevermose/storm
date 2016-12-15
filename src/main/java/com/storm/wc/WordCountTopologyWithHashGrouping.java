package com.storm.wc;

import java.util.concurrent.TimeUnit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * 自定义hash分组
 * @author haiswang
 *
 */
public class WordCountTopologyWithHashGrouping {

    /**
     * @param args
     */
    public static void main(String[] args) {
        
        TopologyBuilder builder=new TopologyBuilder();
        
        builder.setSpout("ReadSpout", new WordCountSpout());
        builder.setBolt("SplitBolt",new WordCountSplitBolt()).shuffleGrouping("ReadSpout");
        
        //设置用户自定义的分组方式
        builder.setBolt("SumBolt", new WordCountSumBolt()).customGrouping("SplitBolt", new HashGrouping());
        
        Config config = new Config();
        config.setDebug(false);
        config.put("filePath", "e:\\word.txt");
        
//        config.setMaxTaskParallelism(1);;  
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
