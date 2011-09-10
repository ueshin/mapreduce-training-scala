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

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

/**
 * @author ueshin
 */
class WordCount1Reducer extends Reducer[Text, LongWritable, Text, LongWritable] {

  private val valueout = new LongWritable()

  /**
   * @param key
   * @param values
   * @param context
   */
  override def reduce(key: Text,
                      values: java.lang.Iterable[LongWritable],
                      context: Reducer[Text, LongWritable, Text, LongWritable]#Context) {
    val count = values.foldLeft(0L) { (cnt, value) =>
      cnt + value.get
    }
    valueout.set(count)
    context.write(key, valueout)
  }

}
