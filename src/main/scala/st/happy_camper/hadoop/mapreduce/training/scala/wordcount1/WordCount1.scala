/*
 * Copyright 2011 Happy-Camper Street.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package st.happy_camper.hadoop.mapreduce.training.scala.wordcount1

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner

import st.happy_camper.hadoop.mapreduce.training.scala.wordcount1.mapreduce.WordCount1Mapper
import st.happy_camper.hadoop.mapreduce.training.scala.wordcount1.mapreduce.WordCount1Reducer

/**
 * @author ueshin
 */
class WordCount1(conf: Configuration = new Configuration) extends Configured(conf) with Tool {

  /**
   * @param inputPaths
   * @param outputPath
   * @return
   */
  def createJob(inputPaths: Seq[Path], outputPath: Path) = {
    new Job(getConf, "WordCount1") {
      setJarByClass(getClass)

      setInputFormatClass(classOf[TextInputFormat])
      FileInputFormat.setInputPaths(this, inputPaths: _*)

      setMapperClass(classOf[WordCount1Mapper])
      setCombinerClass(classOf[WordCount1Reducer])
      setReducerClass(classOf[WordCount1Reducer])

      setOutputKeyClass(classOf[Text])
      setOutputValueClass(classOf[LongWritable])

      setOutputFormatClass(classOf[TextOutputFormat[Text, LongWritable]])
      FileOutputFormat.setOutputPath(this, outputPath)
    }
  }

  /**
   * @param args
   * @return
   */
  def run(args: Array[String]) = {
    if (createJob(args.init.map(new Path(_)), new Path(args.last)).waitForCompletion(true)) 0 else 1
  }
}

/**
 * @author ueshin
 */
object WordCount1 {

  /**
   * @param args
   */
  def main(args: Array[String]) {
    exit(ToolRunner.run(new WordCount1(), args))
  }
}
