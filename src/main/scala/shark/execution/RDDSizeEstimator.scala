/*
 * Copyright (C) 2013 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package shark.execution

import com.google.common.cache.{CacheLoader, CacheBuilder}
import java.io.IOException
import java.net.URI
import java.util.concurrent.TimeUnit
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import spark.RDD
import spark.rdd.HadoopRDD
import spark.Logging
import scala.collection.mutable

object RDDSizeEstimator extends Logging {
  private[this] val hadoopConf = new Configuration()

  private[this] val dirSizeCache = CacheBuilder.newBuilder
    .asInstanceOf[CacheBuilder[String, Long]]
    .expireAfterAccess(60, TimeUnit.MINUTES)
    .build(
      new CacheLoader[String, Long]() {
        override def load(dirPath: String): Long = {
          try {
            val fs = FileSystem.get(new URI(dirPath), hadoopConf)
            fs.getContentSummary(new Path(dirPath)).getLength
          } catch {
            case ex: IOException =>
              logWarning("Could not determine directory size: " + dirPath)
            0
          }
        }
      }
    )

  def estimateInputDataSize(rdd: RDD[_]): Long = {
    val visited = new mutable.HashSet[RDD[_]]

    def visit(r: RDD[_]): Long = {
      if (!visited(r)) {
        visited += r
        rdd match {
          case hadoopRDD: HadoopRDD[_, _] =>
            val conf = hadoopRDD.getConf
            val inputDir = conf.get("mapred.input.dir")
            if (inputDir == null) 0 else dirSizeCache.get(inputDir)
          case _ => rdd.dependencies.view.map {
            dependency => estimateInputDataSize(dependency.rdd)
          }.sum
        }
      } else 0
    }

    visit(rdd)
  }

}
