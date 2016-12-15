package com.storm.wc;

import java.util.concurrent.TimeUnit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopologyWithFieldGrouping {

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
        //fieldsGrouping为域数据流组,通过该种分组方式分发,就会将同一个word分发到同一个实例上
        //这边的new Fields中的参数就是WordCountSplitBolt中declareOutputFields()方法中设置的名称,NOTE:域集合必须存在于数据源的域声明中
        builder.setBolt("SumBolt", new WordCountSumBolt()).fieldsGrouping("SplitBolt", new Fields("word"));
        
        
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
