package com.storm.wc;


import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * 集群模式提交
 * @author haiswang
 *
 */
public class WordCountTopologyWithStormSubmitter {

    /**
     * @param args
     */
    public static void main(String[] args) {
        
        TopologyBuilder builder=new TopologyBuilder();
        
        /**
         * 这边设置两个Spout,也就是说Storm支持多个数据源
         */
        builder.setSpout("ReadSpout", new WordCountSpout());
        builder.setBolt("SplitBolt",new WordCountSplitBolt()).shuffleGrouping("ReadSpout");
        builder.setBolt("SumBolt", new WordCountSumBolt()).shuffleGrouping("SplitBolt");
        
        Config config = new Config();
        config.setDebug(false);
        config.put("filePath", "/home/bigdata/tmpdata/word.txt");
        
//        config.setMaxTaskParallelism(1);;  
//        LocalCluster cluster=new LocalCluster();
        
        try {
            StormSubmitter.submitTopology("word-count",config,builder.createTopology());
        } catch (AlreadyAliveException e1) {
            e1.printStackTrace();
        } catch (InvalidTopologyException e1) {
            e1.printStackTrace();
        }  
        
//        try {
//            TimeUnit.SECONDS.sleep(10);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        
//        //关闭集群
//        cluster.shutdown(); 
        
    }

}
