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

import org.apache.hadoop.conf.Configuration
import org.scalatest.{FunSuite, Matchers}

/**
 * Check validation of parameter config
 */
class ParametersSuite extends FunSuite with Matchers {

  test("Minimal valid parameter map is accepted") {
    val params =
      Map(
        "tempdir" -> "s3://foo/bar",
        "dbtable" -> "test_table",
        "url" -> "jdbc:postgresql://foo/bar")

    val mergedParams = Parameters.mergeParameters(params, new Configuration())

    mergedParams.tempPath should startWith (params("tempdir"))
    mergedParams.jdbcUrl shouldBe params("url")
    mergedParams.table shouldBe params("dbtable")

    // Check that the defaults have been added
    Parameters.DEFAULT_PARAMETERS foreach {
      case (k, v) => mergedParams.parameters(k) shouldBe v
    }
  }

  test("New instances have distinct temp paths") {
    val params =
      Map(
        "tempdir" -> "s3://foo/bar",
        "dbtable" -> "test_table",
        "url" -> "jdbc:postgresql://foo/bar")

    val mergedParams1 = Parameters.mergeParameters(params, new Configuration())
    val mergedParams2 = Parameters.mergeParameters(params, new Configuration())

    mergedParams1.tempPath should not equal mergedParams2.tempPath
  }

  test("Errors are thrown when mandatory parameters are not provided") {

    def checkMerge(params: Map[String, String]): Unit = {
      intercept[Exception] {
        Parameters.mergeParameters(params, new Configuration())
      }
    }

    checkMerge(Map("dbtable" -> "test_table", "url" -> "jdbc:postgresql://foo/bar"))
    checkMerge(Map("tempdir" -> "s3://foo/bar", "url" -> "jdbc:postgresql://foo/bar"))
    checkMerge(Map("dbtable" -> "test_table", "tempdir" -> "s3://foo/bar"))
  }

  test("Options overwrite hadoop configuration") {
    val params =
      Map(
        "tempdir" -> "s3://foo/bar",
        "dbtable" -> "test_table",
        "url" -> "jdbc:postgresql://foo/bar",
        "aws_access_key_id" -> "keyId1",
        "aws_secret_access_key" -> "secretKey1")

    val hadoopConfiguration = new Configuration()
    hadoopConfiguration.set("fs.s3.awsAccessKeyId", "keyId2")
    hadoopConfiguration.set("fs.s3.awsSecretAccessKey", "secretKey2")

    val mergedParams = Parameters.mergeParameters(params, hadoopConfiguration)

    mergedParams.credentialsString shouldBe s"aws_access_key_id=keyId1;aws_secret_access_key=secretKey1"
    mergedParams.hadoopConfiguration.get("fs.s3.awsAccessKeyId") shouldBe "keyId1"
    mergedParams.hadoopConfiguration.get("fs.s3.awsSecretAccessKey") shouldBe "secretKey1"
  }
}
