/*
 * Copyright (c) 2014 the original author or authors.
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

package org.mercuree.transformations.core

import org.scalatest.FlatSpec
import scala.Some

/**
 * [[LocalTransformation]] test.
 *
 * @author Alexander Valyugin
 */
class LocalTransformationSpec extends FlatSpec {

  private val InvalidRootTagXml = <evolution></evolution>
  private val NoNameSpecifiedXml = <transformation></transformation>
  private val UpdateTagMissedXml = <transformation></transformation>
  private val NoUpdateScriptXml = <transformation><update></update></transformation>
  private val NoRollbackScriptXml = <transformation><update>script</update></transformation>
  private val DisabledXml = <transformation enabled="false"><update>script</update></transformation>
  private val SkippedXml = <transformation><update>script</update></transformation>

  "A transformation construction" should "fail if the root tag is invalid" in {
    intercept[TransformationException] {
      LocalTransformation.parseXML(InvalidRootTagXml, "id")
    }
  }

  it should "fail if the name is not specified" in {
    intercept[TransformationException] {
      LocalTransformation.parseXML(NoNameSpecifiedXml, "id")
    }
  }

  it should "fail if the update tag is missing" in {
    intercept[TransformationException] {
      LocalTransformation.parseXML(UpdateTagMissedXml, "id")
    }
  }

  it should "fail if the update script is not specified" in {
    intercept[TransformationException] {
      LocalTransformation.parseXML(NoUpdateScriptXml, "id")
    }
  }

  it should "not fail if the rollback script is missing" in {
    val transformation = LocalTransformation.parseXML(NoRollbackScriptXml, "id")
    transformation match {
      case LocalTransformation(_, _, "", _, _, _) => assert(true)
      case _ => assert(false, "Rollback script should not be mandatory")
    }
  }

  "A transformation" should "be loaded from the url correctly" in {
    val url = getClass.getResource("/transformations/1.0/create_table.sql")
    val transformation = LocalTransformation.fromURL(url, "id")

    transformation match {
      case t: LocalTransformation =>
        assert("id" === t.id)
        assert(t.updateScript ===
          """
            |CREATE TABLE User (
            |    id bigint(20) NOT NULL AUTO_INCREMENT,
            |    email varchar(255) NOT NULL,
            |    password varchar(255) NOT NULL,
            |    fullname varchar(255) NOT NULL,
            |    isAdmin boolean NOT NULL,
            |    PRIMARY KEY (id)
            |);
          """.stripMargin.trim)
        assert("0E5BEB7344F44C053094BEAD4411B621" === t.updateScriptHash.toUpperCase)
        assert("DROP TABLE User;" === t.rollbackScript)
        assert("BCB8140B058A8CA2F5DCA6BF6B26B4B9" === t.rollbackScriptHash.toUpperCase)
        assert(t.runOnChange, "Should defaults to 'true'")
        assert(!t.runAlways, "Should defaults to 'false'")
        assert(t.runInTransaction, "Should defaults to 'true")
      case _ => assert(false, "Should be instanceof LocalTransformation")
    }

  }

  "Disabled transformation" should "be loaded properly" in {
    val transformation = LocalTransformation.parseXML(DisabledXml, "id")

    transformation match {
      case DisabledTransformation(_) => assert(true)
      case _ => assert(false, "Should be instanceof LocalTransformation")
    }
  }

  "Transformation id started with '-'" should "be skipped" in {
    val transformation = LocalTransformation.parseXML(SkippedXml, "-test")

    transformation match {
      case SkippedTransformation("test") => assert(true)
      case _ => assert(false, "Transformation should be skipped")
    }
  }

}
