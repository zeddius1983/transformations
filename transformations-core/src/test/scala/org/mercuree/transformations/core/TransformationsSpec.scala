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
import org.scalamock.scalatest.MockFactory

/**
 * [[Transformations]] test.
 * <p>
 *
 * @author Alexander Valyugin
 */
class TransformationsSpec extends FlatSpec with MockFactory {

  trait MockStoredTransformations extends StoredTransformations {

    val mocked = stub[StoredTransformations]

    override def findAllExcept(names: Set[String]): Seq[StoredTransformation] = mocked.findAllExcept(names)

    override def findById(name: String): Option[StoredTransformation] = mocked.findById(name)

    override def insert(transformation: LocalTransformation): Unit = mocked.insert(transformation)

    override def delete(transformation: Transformation): Unit = mocked.delete(transformation)

    override def update(transformation: LocalTransformation): Unit = mocked.update(transformation)

    override def applyScript(script: String): Unit = mocked.applyScript(script)

    override def transform[A](f: => A): A = f

    override def transactional[A](f: => A): A = f

  }

  class TestTransformations(override val localTransformations: List[Transformation])
    extends Transformations with LocalTransformations with MockStoredTransformations

  "A new transformation" should "be applied" in {
    val local = LocalTransformation("test", "create", "", ApplyMode.Once, true)
    val pack = new TestTransformations(List(local))

    import pack.mocked._
    (findById _).when("test").returns(None)
    (findAllExcept _).when(Set("test")).returns(Nil)

    pack.run

    inSequence {
      (findById _).verify("test")
      (applyScript _).verify("create")
      (insert _).verify(local)
    }
  }

  "Disabled transformation" should "not be applied" in {
    val disabled = DisabledTransformation("test")
    val pack = new TestTransformations(List(disabled))

    import pack.mocked._
    (findById _).when("test").returns(None)
    (findAllExcept _).when(Set("test")).returns(Nil)

    pack.run

    (applyScript _).verify(*).never
    (insert _).verify(*).never
    (delete _).verify(*).never
    (update _).verify(*).never
  }

  "Disabled transformation" should "be rolled back if had been applied previously" in {
    val disabled = DisabledTransformation("test")
    val stored = StoredTransformation("test", "", "", "rollback", "")
    val pack = new TestTransformations(List(disabled))

    import pack.mocked._
    (findById _).when("test").returns(Some(stored))
    (findAllExcept _).when(Set("test")).returns(Nil)

    pack.run

    inSequence {
      (findById _).verify("test")
      (applyScript _).verify("rollback")
      (delete _).verify(stored)
    }
  }

  "Locally removed transformation" should "be rolled back" in {
    val stored = StoredTransformation("test", "", "", "rollback", "")
    val pack = new TestTransformations(List())

    import pack.mocked._
    (findById _).when("test").returns(Some(stored))
    (findAllExcept _).when(Set[String]()).returns(List(stored))

    pack.run

    inSequence {
      (findById _).verify("test")
      (applyScript _).verify("rollback")
      (delete _).verify(stored)
    }
  }

  "Modified transformation" should "be rolled back and applied again" in {
    val local = LocalTransformation("test", "update", "", ApplyMode.Modified, true)
    val stored = StoredTransformation("test", "", "", "rollback", local.rollbackScriptHash)
    val pack = new TestTransformations(List(local))

    import pack.mocked._
    (findById _).when("test").returns(Some(stored))
    (findAllExcept _).when(Set("test")).returns(Nil)

    pack.run

    inSequence {
      (findById _).verify("test")
      (applyScript _).verify("rollback")
      (applyScript _).verify("update")
      (update _).verify(local)
    }
  }

  "Modified transformation" should "not be applied if set to apply once" in {
    val local = LocalTransformation("test", "update", "", ApplyMode.Once, true)
    val stored = StoredTransformation("test", "", "", "rollback", local.rollbackScriptHash)
    val pack = new TestTransformations(List(local))

    import pack.mocked._
    (findById _).when("test").returns(Some(stored))
    (findAllExcept _).when(Set("test")).returns(Nil)

    pack.run

    inSequence {
      (findById _).verify("test")
    }
  }

  "Run always transformation" should "be rolled back and applied again" in {
    val local = LocalTransformation("test", "update", "rollback", ApplyMode.Always, true)
    val stored = StoredTransformation("test", "update", local.updateScriptHash, "rollback", local.rollbackScriptHash)
    val pack = new TestTransformations(List(local))

    import pack.mocked._
    (findById _).when("test").returns(Some(stored))
    (findAllExcept _).when(Set("test")).returns(Nil)

    pack.run

    inSequence {
      (findById _).verify("test")
      (applyScript _).verify("rollback")
      (applyScript _).verify("update")
      (update _).verify(local)
    }
  }

  "If rollback script modified it" should "only update the stored transformation" in {
    val local = LocalTransformation("test", "", "A", ApplyMode.Once, true)
    val stored = StoredTransformation("test", "", local.updateScriptHash, "", "")
    val pack = new TestTransformations(List(local))

    import pack.mocked._
    (findById _).when("test").returns(Some(stored))
    (findAllExcept _).when(Set("test")).returns(Nil)

    pack.run

    inSequence {
      (findById _).verify("test")
      (update _).verify(local)
    }
  }

  "Transformations" should "be applied in the given order" in {
    val local1 = LocalTransformation("test1", "update1", "", ApplyMode.Once, true)
    val local2 = LocalTransformation("test2", "update2", "", ApplyMode.Once, true)
    val stored = StoredTransformation("test3", "", "", "rollback", "")
    val pack = new TestTransformations(List(local2, local1))

    import pack.mocked._
    (findById _).when("test1").returns(None)
    (findById _).when("test2").returns(None)
    (findById _).when("test3").returns(Some(stored))
    (findAllExcept _).when(Set("test1", "test2")).returns(List(stored))

    pack.run

    inSequence {
      (findById _).verify("test2")
      (applyScript _).verify("update2")
      (insert _).verify(local2)
      (findById _).verify("test1")
      (applyScript _).verify("update1")
      (insert _).verify(local1)
      (findById _).verify("test3")
      (applyScript _).verify("rollback")
      (delete _).verify(stored)
    }
  }

}
