package com.storm.wc;

import java.util.concurrent.TimeUnit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {

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
        
        //shuffleGrouping的分组方式是指以随机的方式把元祖分发到下一个bolt上,保证每个消费者收到近似数量的元祖(shuffleGrouping也是最常用的)
        builder.setBolt("SplitBolt",new WordCountSplitBolt()).shuffleGrouping("ReadSpout");
        
        //builder.setBolt("SumBolt", new WordCountSumBolt()).shuffleGrouping("SplitBolt");
        
        //fieldsGrouping为域数据流组,通过该种分组方式分发,就会将同一个word分发到同一个实例上
        //这边的new Fields中的参数就是WordCountSplitBolt中declareOutputFields()方法中设置的名称,NOTE:域集合必须存在于数据源的域声明中
//        builder.setBolt("SumBolt", new WordCountSumBolt())
//               .fieldsGrouping("SplitBolt", new Fields("word"))
//               //用来接收SignalsSpout数据流发送到所有bolt实例的每个元祖
//               .allGrouping("SignalsSpout", "signals");
        
        builder.setBolt("SumBolt", new WordCountSumBolt())
               //设置用户自定义的分组方式
               .customGrouping("SplitBolt", new HashGrouping())
               .allGrouping("SignalsSpout", "signals");
                
        
        
        //Config对象会在运行时与集群的配置进行合并,并通过prepare方法发送给所有的节点
        Config config = new Config();
        config.setDebug(false);
        config.put("filePath", "e:\\word.txt");
        
        config.setMaxTaskParallelism(1);;  
        LocalCluster cluster=new LocalCluster();  
        cluster.submitTopology("word-count",config,builder.createTopology());  
        
        //在生产环境中拓扑会持续运行,但是在这个例子中,只需要运行几秒就能看到结果
        //拓扑运行20s(拓扑会在另外的线程中运行)
        try {
            TimeUnit.SECONDS.sleep(20);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        //关闭集群
        cluster.shutdown(); 
        
    }

}
