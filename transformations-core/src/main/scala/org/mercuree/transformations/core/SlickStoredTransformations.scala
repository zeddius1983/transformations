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

import scala.slick.driver.{JdbcDriver, JdbcProfile}
import scala.slick.jdbc.JdbcBackend._
import scala.slick.jdbc.{StaticQuery => Sql}
import scala.language.implicitConversions
import org.slf4j.LoggerFactory
import Database.dynamicSession

/**
 * Slick based implementation.
 *
 * @author Alexander Valyugin
 */
trait SlickStoredTransformations extends StoredTransformations {

  private final val logger = LoggerFactory.getLogger(getClass)

  val transformationsTableName = "transformations"

  val profile: JdbcProfile = GenericDriver

  val db: Database

  import profile.simple._

  /**
   * Database table schema definition. The table that keeps all database transformations applied.
   */
  private class TransformationTable(tag: Tag, tableName: String) extends Table[StoredTransformation](tag, tableName) {
    def name = column[String]("name", O.PrimaryKey, O.DBType("varchar(256)"))

    def sqlUpdate = column[String]("update_script", O.DBType("text"))

    def sqlUpdateHash = column[String]("update_script_hash", O.DBType("char(128)"))

    def sqlRollback = column[String]("rollback_script", O.DBType("text"))

    def sqlRollbackHash = column[String]("rollback_script_hash", O.DBType("char(128)"))

    // TODO: add timestamp
    
    def * = (name, sqlUpdate, sqlUpdateHash, sqlRollback, sqlRollbackHash) <>(StoredTransformation.fromTuple, StoredTransformation.unapply)
  }

  private lazy val storedTransformations = TableQuery[TransformationTable]((tag: Tag) => new TransformationTable(tag, transformationsTableName))

  /**
   * Creates a transformation table if one is missing.
   */
  private def createTransformationTable(): Unit = db.withDynSession {
    import scala.slick.jdbc.meta.MTable
    if (!MTable.getTables.list.exists(_.name.name == transformationsTableName)) {
      logger.debug(s"Transformation table [$transformationsTableName] is missing!")
      storedTransformations.ddl.create
      logger.debug(s"Created transformation table [$transformationsTableName]")
    }
  }

//  override def all(): Seq[StoredTransformation] = storedTransformations.list

  override def findAllExcept(ids: Set[String]): Seq[StoredTransformation] =
    storedTransformations.filterNot(_.name.inSet(ids)).list

  override def findById(name: String): Option[StoredTransformation] =
    storedTransformations.where(_.name === name).firstOption

  override def insert(transformation: LocalTransformation): Unit =
    storedTransformations += transformation

  override def delete(transformation: Transformation): Unit =
    storedTransformations.where(_.name === transformation.id).delete

  override def update(transformation: LocalTransformation): Unit =
    storedTransformations.where(_.name === transformation.id).update(transformation)

  override def applyScript(script: String): Unit = Sql.updateNA(script).execute

  override def transform[A](f: => A): A = {
    createTransformationTable
    db.withDynSession(f)
  }

  override def transactional[A](f: => A): A = dynamicSession.withTransaction(f)

}

/**
 * Slick 'generic' driver. Should be enough because the plain sql is mostly used.
 *
 * @author Alexander Valyugin
 */
object GenericDriver extends JdbcDriver
