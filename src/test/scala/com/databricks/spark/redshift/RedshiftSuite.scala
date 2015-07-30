/*
 * Copyright 2015 TouchType Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.redshift

import java.io.File
import java.sql.Connection
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.jdbc.DefaultJDBCWrapper
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

/**
 * Tests main DataFrame loading and writing functionality
 */
class RedshiftSuite extends FunSuite with Matchers with MockFactory with BeforeAndAfterAll {

  val jdbcUrl = "jdbc:postgresql://foo/bar"
  val tempDir = s"s3://test1:test2@bucket/folder"

  /**
   * Expected parsed output corresponding to the output of testData.
   */
  val expectedData = Array(
    Row(1.toByte, true, TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0), 1234152.123124981,
      1.0f, 42, 1239012341823719L, 23, "Unicode是樂趣", TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1)),
    Row(1.toByte, false, TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0), 0.0, 0.0f, 42, 1239012341823719L, -13, "asdf",
      TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
    Row(0.toByte, null, TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0), 0.0, -1.0f, 4141214, 1239012341823719L, null, "f",
      TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0)),
    Row(0.toByte, false, null, -1234152.123124981, 100000.0f, null, 1239012341823719L, 24, "___|_123", null),
    Row(List.fill(10)(null): _*)).sorted


  /**
   * Spark Context with hadoop file overridden to point at our local test data file for this suite,
   * no-matter what temp directory was generated and requested.
   */
  private var sc: SparkContext = _
  private var conn: Connection = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext("local", "RedshiftSourceSuite")

    conn = DefaultJDBCWrapper.getConnector("org.postgresql.Driver", jdbcUrl, new Properties())()

    conn.prepareStatement(
      """
        |create temporary table test_table (
        | testByte int2,
        | testBool boolean,
        | testDate date,
        | testDouble float8,
        | testFloat float4,
        | testInt int4,
        | testLong int8,
        | testShort int2,
        | testString varchar(256),
        | testTimestamp timestamp
        |)""".stripMargin
    ).executeUpdate()

    conn.prepareStatement(
      """
        |insert into test_table values (
        | 1, true, '2015-07-01', 1234152.123124981, 1.0, 42,
        | 1239012341823719, 23, 'Unicode是樂趣', "2015-07-01 00:00:01.001")
      """.stripMargin
    ).executeUpdate()

    conn.prepareStatement(
      """
        |insert into test_table values (
        | 1, false, '2015-07-02', 0.0, 0.0, 42, 1239012341823719, -13, 'asdf', '2015-07-02 00:00:00.000')
      """.stripMargin
    ).executeUpdate()

    conn.prepareStatement(
      """
        |insert into test_table values (
        | 0, null, '2015-07-03', 0.0, -1.0, 4141214, 1239012341823719, null, 'f', '2015-07-03 00:00:00.000')
      """.stripMargin
    ).executeUpdate()

    conn.prepareStatement(
      """
        |insert into test_table values (
        | 0, false, null, -1234152.123124981, 100000.0, null, 1239012341823719, 24, '___|_123', null)
      """.stripMargin
    ).executeUpdate()

    conn.prepareStatement(
      """
        |insert into test_table values (
        | null, null, null, null, null, null, null, null, null, null)
      """.stripMargin
    ).executeUpdate()

    conn.prepareStatement(
      """
        |create temporary table test_table2 (
        | testByte int2,
        | testBool boolean,
        | testDate date,
        | testDouble float8,
        | testFloat float4,
        | testInt int4,
        | testLong int8,
        | testShort int2,
        | testString varchar(256),
        | testTimestamp timestamp
        |)""".stripMargin
    ).executeUpdate()

    conn.prepareStatement(
      """
        |insert into test_table2 values (
        | 1, true, '2015-07-01', 1234152.123124981, 1.0, 42,
        | 1239012341823719, 23, 'Unicode是樂趣', '2015-07-01 00:00:01.001')
      """.stripMargin
    ).executeUpdate()

    conn.commit()
  }

  override def afterAll(): Unit = {
    val temp = new File(tempDir)
    val tempFiles = temp.listFiles()
    if(tempFiles != null) tempFiles foreach {
      case f => if(f != null) f.delete()
    }
    temp.delete()

    conn.close()

    sc.stop()
    super.afterAll()
  }
  
  test("DefaultSource can load Redshift UNLOAD output to a DataFrame") {
    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      s"""
         |create table test_table(
         |  testByte byte,
         |  testBool boolean,
         |  testDate date,
         |  testDouble double,
         |  testFloat float,
         |  testInt int,
         |  testLong long,
         |  testShort short,
         |  testString string
         |)
         |using com.databricks.spark.redshift
         |options(
         |  url \"$jdbcUrl\",
         |  tempdir \"$tempDir\",
         |  dbtable \"test_table\",
         |)""".stripMargin
    )

    sqlContext.table("test_table").collect().sorted
      .zip(expectedData).foreach {
      case (loaded, expected) =>
        loaded shouldBe expected
    }
  }

  test("DefaultSource supports simple column filtering") {
    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      s"""
         |create table test_table(
         |  testByte byte,
         |  testBool boolean,
         |  testDate date,
         |  testDouble double,
         |  testFloat float,
         |  testInt int,
         |  testLong long,
         |  testShort short,
         |  testString string
         |)
         |using com.databricks.spark.redshift
         |options(
         |  url \"$jdbcUrl\",
         |  tempdir \"$tempDir\",
         |  dbtable \"test_table\"
         |)""".stripMargin
    )

    val prunedExpectedValues =
      Array(Row(1.toByte, true),
        Row(1.toByte, false),
        Row(0.toByte, null),
        Row(0.toByte, false),
        Row(null, null)).sorted

    sqlContext.sql("select testByte, testBool from test_table").collect().sorted
      .zip(prunedExpectedValues).foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }

  test("DefaultSource supports user schema, pruned and filtered scans") {
    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      s"""
         |create table test_table(
         |  testByte byte,
         |  testBool boolean,
         |  testDate date,
         |  testDouble double,
         |  testFloat float,
         |  testInt int,
         |  testLong long,
         |  testShort short,
         |  testString string
         |)
         |using com.databricks.spark.redshift
         |options(
         |  url \"$jdbcUrl\",
         |  tempdir \"$tempDir\",
         |  dbtable \"test_table\"
         |)""".stripMargin
    )

    // We should now only have one matching row, with two columns
    val filteredExpectedValues = Array(Row(1, true))
    sqlContext.sql(
      """
        |select testByte, testBool
        |from test_table
        |where testBool=true
        | and testString='Unicode是樂趣'
        | and testDouble=1000.0
        | and testFloat=1.0f
        | and testInt=43""".stripMargin
    ).collect().zip(filteredExpectedValues).foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }

  test("DefaultSource using 'query' supports user schema, pruned and filtered scans") {
    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      s"""
         |create table test_table(
         |  testByte byte,
         |  testBool boolean,
         |  testDate date,
         |  testDouble double,
         |  testFloat float,
         |  testInt int,
         |  testLong long,
         |  testShort short,
         |  testString string
         |)
         |using com.databricks.spark.redshift
         |options(
         |  url \"$jdbcUrl\",
         |  tempdir \"$tempDir\",
         |  query \"select * from test_table\"
         |)""".stripMargin
    )

    // We should now only have one matching row, with two columns
    val filteredExpectedValues = Array(Row(1, true))

    sqlContext.sql(
      """
        |select testByte, testBool
        |from test_table
        |where testBool=true
        | and testString='Unicode是樂趣'
        | and testDouble=1000.0
        | and testFloat=1.0f
        | and testInt=43""".stripMargin
    ).collect().zip(filteredExpectedValues).foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }

  test("DefaultSource serializes data as Avro, then sends Redshift COPY command") {
    val extraData = Array(
      Row(2.toByte, false, null, -1234152.123124981, 100000.0f, null, 1239012341823719L, 24, "___|_123", null))

    val sqlContext = new SQLContext(sc)
    val rdd = sc.parallelize(extraData.toSeq)
    val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
    df.write.format("com.databricks.spark.redshift").mode(SaveMode.Overwrite).saveAsTable("test_table2")

    sqlContext.sql("select * from test_table2").collect().zip(extraData).foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }

  test("Append SaveMode doesn't destroy existing data") {
    val extraData = Array(
      Row(2.toByte, false, null, -1234152.123124981, 100000.0f, null, 1239012341823719L, 24, "___|_123", null))

    val sqlContext = new SQLContext(sc)
    val rdd = sc.parallelize(extraData.toSeq)
    val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
    df.write.format("com.databricks.spark.redshift").mode(SaveMode.Append).saveAsTable("test_table")

    sqlContext.sql("select * from test_table").collect().sorted
      .zip((expectedData ++ extraData).sorted).foreach {
      case (loaded, expected) => loaded shouldBe expected
    }

    conn.prepareStatement("delete from test_table where testByte=2").executeUpdate()
    conn.commit()
  }

  test("Respect SaveMode.ErrorIfExists when table exists") {
    val sqlContext = new SQLContext(sc)
    val rdd = sc.parallelize(expectedData.toSeq)
    val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)

    // Check that SaveMode.ErrorIfExists throws an exception
    intercept[Exception] {
      df.write.format("com.databricks.spark.redshift").mode(SaveMode.ErrorIfExists).saveAsTable("test_table")
    }
  }

  test("Do nothing when table exists if SaveMode = Ignore") {
    val sqlContext = new SQLContext(sc)
    val rdd = sc.parallelize(expectedData.toSeq)
    val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
    df.write.format("com.databricks.spark.redshift").mode(SaveMode.Ignore).saveAsTable("test_table")

    // Check that SaveMode.Ignore does nothing
    sqlContext.sql("select * from test_table").collect().zip(expectedData).foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }
}
