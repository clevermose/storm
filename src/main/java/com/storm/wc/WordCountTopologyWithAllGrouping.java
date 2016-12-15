package com.storm.wc;

import java.util.concurrent.TimeUnit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopologyWithAllGrouping {

    /**
     * @param args
     */
    public static void main(String[] args) {
        
        TopologyBuilder builder=new TopologyBuilder();
        
        /**
         * 这边设置两个Spout,也就是说Storm支持多个数据源
         */
        builder.setSpout("ReadSpout", new WordCountSpout());
        //设置信号分发Spout
        builder.setSpout("SignalsSpout", new SignalsSpout());
        
        builder.setBolt("SplitBolt",new WordCountSplitBolt()).shuffleGrouping("ReadSpout");
        builder.setBolt("SumBolt", new WordCountSumBolt()).shuffleGrouping("SplitBolt")
               //设置用户自定义的分组方式
               .allGrouping("SignalsSpout", "signals");
                
        
        
        Config config = new Config();
        config.setDebug(false);
        config.put("filePath", "e:\\word.txt");
        
        LocalCluster cluster=new LocalCluster();  
        cluster.submitTopology("word-count",config,builder.createTopology());  
        
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        //关闭集群
        cluster.shutdown(); 
        
    }

}
