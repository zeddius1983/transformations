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

import scala.io.Source
import java.net.URL
import java.security.MessageDigest
import org.slf4j.{Logger, LoggerFactory}
import scala.language.implicitConversions
import java.io.File
import scala.util.Try
import scala.xml.NodeSeq

/**
 * Represents a database change with a unique name.
 */
trait Transformation {
  val id: String
}

/**
 * Transformation with parsed update and rollback scripts and their hash sum.
 */
trait ScriptedTransformation extends Transformation {
  val updateScript: String
  val updateScriptHash: String
  val rollbackScript: String
  val rollbackScriptHash: String
}

/**
 * A transformation that has been already applied to a database.
 */
case class StoredTransformation(id: String, updateScript: String, updateScriptHash: String,
                                rollbackScript: String, rollbackScriptHash: String) extends ScriptedTransformation

object StoredTransformation {
  def fromTuple: ((String, String, String, String, String)) => StoredTransformation = {
    case Tuple5(x1, x2, x3, x4, x5) => apply(x1, x2, x3, x4, x5)
  }
}

/**
 * Transformation that will be skipped during processing.
 */
case class SkippedTransformation(id: String) extends Transformation

/**
 * Disabled transformation is one that will either be rollbacked or skipped during processing.
 */
case class DisabledTransformation(id: String) extends Transformation

/**
 * A user requested transformation to apply.
 */
case class LocalTransformation(id: String, updateScript: String, rollbackScript: String,
                               runOnChange: Boolean = true, runAlways: Boolean = false,
                               runInTransaction: Boolean = true) extends ScriptedTransformation {
  val updateScriptHash = md5(updateScript)
  val rollbackScriptHash = md5(rollbackScript)

  private def md5(text: String) = MessageDigest.getInstance("MD5").digest(text.getBytes).map("%02x".format(_)).mkString
}

object LocalTransformation {

  //  private val IdAttr = "@id" TODO: need it?
  private val EnabledAttr = "@enabled"
  private val RunOnChangeAttr = "@runOnChange"
  private val RunInTransactionAttr = "@runInTransaction" // TODO: what to do with rollback? Do we need it at all?
  private val RunAlwaysAttr = "@runAlways"

  private val RootTag = "transformation"
  private val UpdateTag = "update"
  private val RollbackTag = "rollback"
  // TODO: private val AuthorAttr = "@author" ?
  // TODO: private val DependsOn = "@dependsOn" do we need it?

  implicit def localToStored(local: LocalTransformation) = StoredTransformation(
    local.id, local.updateScript, local.updateScriptHash, local.rollbackScript, local.rollbackScriptHash)

  /**
   * Loads the transformation from the given file.
   *
   * @param file to load from.
   * @param id transformation id.
   * @return transformation object.
   */
  def fromFile(file: File, id: String): Transformation = {
    val source = Source.fromFile(file).mkString
    parseSQL(source, id)
  }

  /**
   * Loads the transformation from the given url.
   *
   * @param url file path.
   * @param id transformation id.
   * @return transformation object.
   */
  def fromURL(url: URL, id: String): Transformation = {
    val source = Source.fromURL(url).mkString
    parseSQL(source, id)
  }

  /**
   * Parses the sql source to obtain a transformation object. Valid sql text to parse may look like this:
   * {{{
   * --<transformation>
   *   --<update>
   *     -- script body here
   *   --</update>
   *   --<rollback>
   *     -- rollback script body here
   *   --</rollback>
   * --</transformation>
   * }}}
   *
   * @param sql sql text.
   * @param id transformation id.
   * @return transformation object.
   */
  def parseSQL(sql: String, id: String): Transformation = {
    val xml = scala.xml.XML.loadString(sql.replace("--<", "<"))
    parseXML(xml, id)
  }

  /**
   * Parses the valid xml to obtain a transformation object.
   *
   * @param xml xml document.
   * @param id transformation id.
   * @return transformation object.
   */
  def parseXML(xml: scala.xml.Elem, id: String): Transformation = {
    if (xml.label != RootTag) {
      throw TransformationException(s"Transformation root element must be <$RootTag> tag")
    }

    def toBoolean(seq: NodeSeq, default: Boolean = true) = seq.map(_.text.trim.toBoolean).headOption.getOrElse(default)

    lazy val enabled = toBoolean(xml \ EnabledAttr)
    lazy val runOnChange = toBoolean(xml \ RunOnChangeAttr)
    lazy val runInTransaction = toBoolean(xml \ RunInTransactionAttr)
    lazy val runAlways = toBoolean(xml \ RunAlwaysAttr, false)

    // Update script is mandatory
    lazy val sqlUpdate = (xml \\ UpdateTag).text.trim match {
      case s if s.nonEmpty => s
      case _ => throw TransformationException(s"Update script must be specified inside <$UpdateTag> tag")
    }

    // Rollback script is not mandatory
    lazy val sqlRollback = (xml \\ RollbackTag).text.trim

    if (id startsWith "-")
      SkippedTransformation(id.substring(1))
    else if (!enabled)
      DisabledTransformation(id)
    else
      LocalTransformation(id, sqlUpdate, sqlRollback, runOnChange, runAlways, runInTransaction)
  }
}

case class TransformationException(message: String) extends RuntimeException(message)

/**
 * Stored transformations component.
 */
trait StoredTransformations {
  /**
   * Finds all stored transformations except those which ids is found in the set.
   *
   * @param ids set of ids.
   * @return sequence of stored transformations.
   */
  def findAllExcept(ids: Set[String]): Seq[StoredTransformation]

  /**
   * Finds the stored transformation by it's id.
   *
   * @param id to look for.
   * @return stored transformation option.
   */
  def findById(id: String): Option[StoredTransformation]

  /**
   * Inserts the local transformation to the underlying storage.
   *
   * @param transformation to insert.
   */
  def insert(transformation: LocalTransformation)

  /**
   * Deletes the transformation from the underlying storage.
   *
   * @param transformation to delete.
   */
  def delete(transformation: Transformation)

  /**
   * Updates the corresponding stored transformation with the details
   * taken from the given local transformation.
   *
   * @param transformation to obtain details.
   */
  def update(transformation: LocalTransformation)

  /**
   * Applies the given script to the underlying database.
   *
   * @param script to apply.
   */
  def applyScript(script: String)

  /**
   * TODO: scaladoc
   * @param f
   * @tparam A
   * @return
   */
  def transform[A](f: => A): A

  /**
   * TODO: scaladoc
   * @param f
   * @tparam A
   * @return
   */
  def transactional[A](f: => A): A
}

/**
 * Local transformations contract.
 */
trait LocalTransformations {
  /**
   * Returns the list of user requested transformations to be applied.
   * Transformations are guaranteed to be applied in the order they follow in the list.
   *
   * @return a list of [[Transformation]].
   */
  def localTransformations: List[Transformation]
}

/**
 * Defines the transformation process algorithm.
 *
 * @author Alexander Valyugin
 */
trait Transformations {this: LocalTransformations with StoredTransformations =>

  private final val logger = LoggerFactory.getLogger(getClass)

  import System.{currentTimeMillis => currentTime}

  private def profile[R](f: => R, t: Long = currentTime) = { f; currentTime - t }

  private def apply(local: LocalTransformation): Unit = transactional(local.runInTransaction) {
    val storedOption = findById(local.id)
    storedOption match {
      case Some(stored) =>
        def rollbackAndUpdate(): Unit = {
          applyScript(stored.rollbackScript)
          applyScript(local.updateScript)
          update(local)
        }

        if (local.runAlways) {
          logger.info(s"> [${local.id}] is set to run always")
          rollbackAndUpdate
        } else if (local.updateScriptHash != stored.updateScriptHash) {
          logger.info(s"> [${local.id}] update script has been modified")
          if (local.runOnChange) {
            rollbackAndUpdate
          } else {
            logger.warn(s"> [${local.id}] is set not to run on change!")
          }
        } else if (local.rollbackScriptHash != stored.rollbackScriptHash) {
          logger.info(s"> [${local.id}] rollback script has been modified")
          update(local)
        }
      case None =>
        logger.info(s"> [${local.id}] is ran for the first time")
        applyScript(local.updateScript)
        insert(local)
    }
  }

  protected def tryApply(local: LocalTransformation): Unit = {
    try {
      logger.info(s"Applying [${local.id}]")
      val elapsed = profile(apply(local))
      // TODO: instead of miliseconds consider human readable time like
      // TODO: 245ms, 2342ms, 11s, 1m 12s, 13m, 1h 5m
      logger.info(s"[${local.id}] processed in $elapsed ms")
    } catch {
      case e: Exception => logger.error(s"Failed to apply [${local.id}] due to:\n ${e.getMessage}")
    }
  }

  protected def onApply(local: LocalTransformation): Unit = tryApply(local)

  // TODO: think if we need to control transactional here
  private def rollback(transformation: Transformation): Unit = transactional {
    val storedOption = findById(transformation.id)
    storedOption map { stored =>
      applyScript(stored.rollbackScript)
      delete(stored)
    }
  }

  protected def tryRollback(transformation: Transformation): Unit = {
    try {
      logger.info(s"Rolling back [${transformation.id}]")
      val elapsed = profile(rollback(transformation))
      logger.info(s"[${transformation.id}] processed in $elapsed ms")
    } catch {
      case e: Exception => logger.error(s"Failed to rollback [${transformation.id}}] due to:\n ${e.getMessage}")
    }
  }

  protected def onRollback(transformation: Transformation): Unit = tryRollback(transformation)

  private def transactional[A](enabled: Boolean)(f: => A): Unit = if (enabled) transactional(f) else f

  /**
   * Runs transformations.
   */
  def run {
    val locals = localTransformations
    transform {
      // First apply local transformations as ordered in the list
      locals.foreach {
        case SkippedTransformation(id) => logger.debug(s"Transformation [$id] skipped")
        case disabled: DisabledTransformation =>
          logger.debug(s"Transformation [${disabled.id}] is disabled")
          onRollback(disabled)
        case local: LocalTransformation =>
          logger.debug(s"Transformation [${local.id}] is found")
          onApply(local)
      }

      // Second rollback transformations missing locally
      findAllExcept(locals.map(_.id).toSet).foreach { stored =>
        logger.debug(s"Transformation [${stored.id}] is missing")
        onRollback(stored)
      }
    }
  }

}