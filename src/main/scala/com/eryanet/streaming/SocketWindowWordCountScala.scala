package com.eryanet.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @BelongsProject: flink-quickstart
 * @BelongsPackage: com.eryanet
 * @Author: zhangqiang
 * @CreateTime: 2020-12-21 14:17
 * @Description:
 */
object SocketWindowWordCountScala {

  def main(args: Array[String]): Unit = {
    val tool: ParameterTool = ParameterTool.fromArgs(args)

    // 获取host
    val hostname: String = try {
      tool.get("host")
    } catch {
      case e: Exception => {
        System.err.println("no host set, use default localhost --- scala")
      }
        "localhost"
    }

    // 获取Socket端口号
    val port: Int = try {
      tool.getInt("port")
    } catch {
      case e:Exception => {
        System.err.println("no port set, use default 9000 --- scala")
      }
        9000
    }

    // 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 连接Socket获取输入数据
    val text = env.socketTextStream(hostname, port, '\n')

    // 解析数据，把数据打平，分组，窗口统计，并且聚合求sum

    // 必须添加这一行饮食转换，否则下面的FlatMap方法执行会报错
    import org.apache.flink.api.scala._

    val windowCount = text
      // 打平，把每一行单词都切开
      .flatMap(line => line.split("\\s"))
      // 把单词转换为 word，1 这种形式
      .map(w => WordWithCount(w, 1))
      // 分组
      .keyBy("word")
      // 指定窗口大小，指定间隔时间
      .timeWindow(Time.seconds(2), Time.seconds(1))
      // sum或者reduce都可以
//      .sum("count")
      .reduce((a,b) => WordWithCount(a.word, a.count + b.count))

    // 打印到控制台
    windowCount.print().setParallelism(1)

    // 执行任务
    env.execute("Socket Window WordCount")
  }

  case class WordWithCount(word: String, count: Long)
}
