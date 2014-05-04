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
import scala.util.Random

/**
 * [[FileLocalTransformations]] test.
 *
 * @author Alexander Valyugin
 */
class FileLocalTransformationsSpec extends FlatSpec {

  class TestTransformations(override val transformationsPath: String) extends FileLocalTransformations

  "Paths" should "follow in the given order" in {
    val expectedPaths = List(
      "init.sql",
      "1.0.0/table.sql",
      "1.0.0/common/clean.sql",
      "1.0.1/alter.sql",
      "2.1/new.sql",
      "2.1/1.0/some.sql",
      "2.1/2/another.sql",
      "2013.2.1/drop.sql",
      "2013.2.1/erase.sql",
      "2014/duplicate.sql",
      "assembly/ddl.sql",
      "common/houseKeep.sql",
      "common/dev/cleanup.sql"
    )
    val actualPaths = Random.shuffle(expectedPaths) sorted FilePathOrdering

    expectedPaths zip actualPaths foreach (it => assert(it._1 === it._2))
  }

  "Local transformations" should "be loaded from the classpath" in {
    val pack = new TestTransformations("/transformations")
    val local = pack.localTransformations

    assert(3 === local.length)
    assert("1.0/create_table.sql" === local(0).id)
    assert("2.0/add_column.xml" === local(1).id)
    assert("3.0/add_data.sql" === local(2).id)
  }

}
