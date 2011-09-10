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
package st.happy_camper.hadoop.mapreduce.training.scala.wordcount1.mapreduce

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

/**
 * @author ueshin
 */
class WordCount1Mapper extends Mapper[LongWritable, Text, Text, LongWritable] {

  private val keyout = new Text()
  private val valueout = new LongWritable(1L)

  /**
   * @param key
   * @param value
   * @param context
   */
  override def map(key: LongWritable,
                   value: Text,
                   context: Mapper[LongWritable, Text, Text, LongWritable]#Context) {
    value.toString.split("\\W").collect {
      case word if word != "" =>
        keyout.set(word)
        context.write(keyout, valueout)
    }
  }

}
