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

import org.scalatest.{Assertions, BeforeAndAfterAll, FunSuite}
import shark.execution.RDDSizeEstimator
import shark.{SharkEnv, TestUtils, SharkContext}

class RDDSizeSuite extends FunSuite with BeforeAndAfterAll with Assertions {

  private[this] var sc: SharkContext = _

  private[this] val WAREHOUSE_PATH = TestUtils.getWarehousePath()
  private[this] val METASTORE_PATH = TestUtils.getMetastorePath()

  override def beforeAll() {
    sc = SharkEnv.initWithSharkContext("rdd-size-suite", "local")
    sc.sql("set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" +
        METASTORE_PATH + ";create=true")
    sc.sql("set hive.metastore.warehouse.dir=" + WAREHOUSE_PATH)
    sc.sql("set shark.test.data.path=" + TestUtils.dataFilePath)

    sc.sql("drop table if exists test")
    sc.sql("CREATE TABLE test (key INT, val STRING)")
    sc.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/kv1.txt' INTO TABLE test")
  }

  override def afterAll() {
    SharkEnv.stop()
    System.clearProperty("spark.driver.port")
  }

  test("rdd size") {
    val rdd = sc.sql2rdd("select * from test")
    val rddSize = RDDSizeEstimator.estimateInputDataSize(rdd)
    val minExpected = 5686
    val maxExpected = 5868
    assert(minExpected <= rddSize && rddSize <= maxExpected,
           "Expected RDD size between " + minExpected + " and " + maxExpected + " but was " +
           rddSize)
  }

}
