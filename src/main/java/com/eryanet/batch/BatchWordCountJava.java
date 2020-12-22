package com.eryanet.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @BelongsProject: flink-quickstart
 * @BelongsPackage: com.eryanet.batch
 * @Author: zhangqiang
 * @CreateTime: 2020-12-21 16:41
 * @Description:
 */
public class BatchWordCountJava {

    public static void main(String[] args) throws Exception {
        String inputPath = "/Users/zhangqiang/Desktop/testBatch.txt";
        String outPath = "/Users/zhangqiang/Desktop/testBatch-result.txt";

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //获取文件中的内容
        DataSource<String> text = env.readTextFile(inputPath);
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);

        counts.writeAsCsv(outPath,"\n"," ").setParallelism(1);
        env.execute("batch word count");
    }

    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>> {
//        @Override
//        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception { }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token: tokens) {
                if (token.length()>0) {
                    out.collect(new Tuple2<String, Integer>(token,1));
                }
            }
        }
    }
}


