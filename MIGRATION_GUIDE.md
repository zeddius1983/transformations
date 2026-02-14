# Migration Guide: Modernizing the Transformations Framework

This guide helps you upgrade the Transformations Framework from Scala 2.10 to modern versions (2.13 or 3.x) and update dependencies.

## Table of Contents

- [Overview](#overview)
- [Version Comparison](#version-comparison)
- [Migration Path](#migration-path)
- [Step-by-Step Instructions](#step-by-step-instructions)
- [Code Changes](#code-changes)
- [Breaking Changes](#breaking-changes)
- [Testing Your Migration](#testing-your-migration)
- [Dependency Updates](#dependency-updates)
- [Build Configuration](#build-configuration)
- [Migration Checklist](#migration-checklist)

## Overview

The current framework uses legacy versions:
- **Scala 2.10.3** (released 2013, EOL)
- **Slick 2.0.0** (released 2013, EOL)
- **Gradle 1.10** (released 2013)

This guide covers upgrading to modern, maintained versions while preserving all functionality.

### Why Upgrade?

- ✅ **Security**: Get latest security patches
- ✅ **Performance**: Benefit from runtime improvements
- ✅ **Features**: Access new Scala and Slick features
- ✅ **Support**: Use actively maintained libraries
- ✅ **Ecosystem**: Compatibility with modern tools
- ✅ **Future-proof**: Prepare for Scala 3

## Version Comparison

| Component | Current | Scala 2.13 Target | Scala 3.x Target |
|-----------|---------|-------------------|------------------|
| **Scala** | 2.10.3 | 2.13.12 | 3.3.1 |
| **Slick** | 2.0.0 | 3.4.1 | 3.4.1 |
| **Java** | 6 | 8+ | 8+ |
| **Gradle** | 1.10 | 8.5 | 8.5 |
| **SBT** | N/A | 1.9.7 | 1.9.7 |

### API Compatibility

| Feature | Scala 2.10 | Scala 2.13 | Scala 3.x | Notes |
|---------|------------|------------|-----------|-------|
| Traits | ✓ | ✓ | ✓ | No change |
| Case classes | ✓ | ✓ | ✓ | No change |
| Implicits | ✓ | ✓ | Given/Using | Syntax change in Scala 3 |
| XML literals | ✓ | Library | Library | Must add scala-xml dependency |
| Pattern matching | ✓ | ✓ | ✓ | Minor syntax improvements |

## Migration Path

We recommend a **gradual migration**:

```
Scala 2.10.3
    ↓
Scala 2.13.x  ← Start here (lowest risk)
    ↓
Scala 3.x     ← Optional (if desired)
```

### Recommended Approach

1. **Phase 1**: Migrate to Scala 2.13 + Slick 3.x
2. **Phase 2**: Test thoroughly
3. **Phase 3**: (Optional) Migrate to Scala 3

## Step-by-Step Instructions

### Phase 1: Migrate to Scala 2.13

#### Step 1: Update Build Configuration

**Gradle** (`build.gradle`):

```gradle
// Before (Scala 2.10)
subprojects {
    apply plugin: 'scala'
    
    ext {
        scalaVersion = "2.10.3"
        slickVersion = "2.0.0"
    }
    
    dependencies {
        compile "org.scala-lang:scala-library:${scalaVersion}"
        compile "com.typesafe.slick:slick_2.10:${slickVersion}"
    }
    
    sourceCompatibility = 1.6
    targetCompatibility = 1.6
}

// After (Scala 2.13)
subprojects {
    apply plugin: 'scala'
    
    ext {
        scalaVersion = "2.13.12"
        slickVersion = "3.4.1"
    }
    
    dependencies {
        implementation "org.scala-lang:scala-library:${scalaVersion}"
        implementation "com.typesafe.slick:slick_${scalaVersion.take(4)}:${slickVersion}"
        implementation "org.scala-lang.modules:scala-xml_${scalaVersion.take(4)}:2.1.0"
        
        // Slick 3.x requires HikariCP
        implementation "com.typesafe.slick:slick-hikaricp_${scalaVersion.take(4)}:${slickVersion}"
    }
    
    sourceCompatibility = 1.8
    targetCompatibility = 1.8
}
```

**SBT** (`build.sbt` - if migrating to SBT):

```scala
// Scala 2.13 version
lazy val root = (project in file("."))
  .settings(
    name := "transformations",
    version := "1.0.0",
    scalaVersion := "2.13.12",
    
    libraryDependencies ++= Seq(
      "com.typesafe.slick" %% "slick" % "3.4.1",
      "com.typesafe.slick" %% "slick-hikaricp" % "3.4.1",
      "org.scala-lang.modules" %% "scala-xml" % "2.1.0",
      "com.h2database" % "h2" % "2.2.224",
      "org.slf4j" % "slf4j-api" % "2.0.9",
      "ch.qos.logback" % "logback-classic" % "1.4.11" % Runtime,
      
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.scalamock" %% "scalamock" % "5.2.0" % Test
    )
  )
```

#### Step 2: Update Slick Imports

Slick 3.x has different import structure:

```scala
// Before (Slick 2.0)
import scala.slick.driver.{JdbcDriver, JdbcProfile}
import scala.slick.jdbc.JdbcBackend._
import scala.slick.jdbc.{StaticQuery => Sql}
import Database.dynamicSession

// After (Slick 3.4)
import slick.jdbc.{JdbcProfile, JdbcBackend}
import slick.jdbc.GetResult
import JdbcBackend._
```

#### Step 3: Update Driver References

```scala
// Before (Slick 2.0)
val profile: JdbcProfile = GenericDriver
import profile.simple._

object GenericDriver extends JdbcDriver

// After (Slick 3.4)
val profile: JdbcProfile = slick.jdbc.H2Profile
import profile.api._
```

#### Step 4: Update Table Definitions

```scala
// Before (Slick 2.0)
private class TransformationTable(tag: Tag, tableName: String) 
  extends Table[StoredTransformation](tag, tableName) {
  
  def name = column[String]("name", O.PrimaryKey, O.DBType("varchar(256)"))
  def sqlUpdate = column[String]("update_script", O.DBType("text"))
  
  def * = (name, sqlUpdate, ...) <>(StoredTransformation.fromTuple, StoredTransformation.unapply)
}

// After (Slick 3.4)
private class TransformationTable(tag: Tag, tableName: String) 
  extends Table[StoredTransformation](tag, tableName) {
  
  def name = column[String]("name", O.PrimaryKey, O.SqlType("varchar(256)"))
  def sqlUpdate = column[String]("update_script", O.SqlType("text"))
  
  def * = (name, sqlUpdate, ...) <> (StoredTransformation.tupled, StoredTransformation.unapply)
}
```

#### Step 5: Update Database Session Handling

Major change in Slick 3.x - no more dynamic sessions:

```scala
// Before (Slick 2.0)
import Database.dynamicSession

def findById(name: String): Option[StoredTransformation] =
  storedTransformations.where(_.name === name).firstOption

override def transform[A](f: => A): A = {
  createTransformationTable
  db.withDynSession(f)
}

override def transactional[A](f: => A): A = 
  dynamicSession.withTransaction(f)

// After (Slick 3.4)
def findById(name: String): Option[StoredTransformation] = {
  import profile.api._
  db.run(storedTransformations.filter(_.name === name).result.headOption)
    .await
}

override def transform[A](f: => A): A = {
  createTransformationTable()
  f
}

override def transactional[A](f: => A): A = {
  import scala.concurrent.Await
  import scala.concurrent.duration._
  import profile.api._
  
  Await.result(
    db.run(DBIO.seq(f).transactionally),
    Duration.Inf
  )
}
```

#### Step 6: Update Query Methods

```scala
// Before (Slick 2.0)
storedTransformations.filterNot(_.name.inSet(ids)).list
storedTransformations.where(_.name === name).firstOption
storedTransformations += transformation
storedTransformations.where(_.name === name).delete
storedTransformations.where(_.name === name).update(transformation)

// After (Slick 3.4)
import scala.concurrent.Await
import scala.concurrent.duration._
import profile.api._

Await.result(
  db.run(storedTransformations.filterNot(_.name inSet ids).result),
  Duration.Inf
)
Await.result(
  db.run(storedTransformations.filter(_.name === name).result.headOption),
  Duration.Inf
)
Await.result(
  db.run(storedTransformations += transformation),
  Duration.Inf
)
Await.result(
  db.run(storedTransformations.filter(_.name === name).delete),
  Duration.Inf
)
Await.result(
  db.run(storedTransformations.filter(_.name === name).update(transformation)),
  Duration.Inf
)
```

#### Step 7: Update SQL Execution

```scala
// Before (Slick 2.0)
import scala.slick.jdbc.{StaticQuery => Sql}
Sql.updateNA(script).execute

// After (Slick 3.4)
import slick.jdbc.GetResult
import scala.concurrent.Await
import scala.concurrent.duration._

Await.result(
  db.run(sqlu"#$script"),
  Duration.Inf
)
```

#### Step 8: Add XML Support

Scala 2.13 removed XML from standard library:

```scala
// Add to dependencies
"org.scala-lang.modules" %% "scala-xml" % "2.1.0"

// Imports remain the same
import scala.xml.{XML, Elem, NodeSeq}
```

### Phase 2: Migrate to Scala 3 (Optional)

#### Step 1: Update Scala Version

```gradle
ext {
    scalaVersion = "3.3.1"
}

dependencies {
    implementation "org.scala-lang:scala3-library_3:${scalaVersion}"
}
```

#### Step 2: Update Implicit Conversions

```scala
// Before (Scala 2)
import scala.language.implicitConversions

implicit def localToStored(local: LocalTransformation): StoredTransformation = 
  StoredTransformation(...)

// After (Scala 3)
given Conversion[LocalTransformation, StoredTransformation] with
  def apply(local: LocalTransformation): StoredTransformation = 
    StoredTransformation(...)
```

#### Step 3: Update Pattern Matching

```scala
// Before (Scala 2)
object StoredTransformation {
  def fromTuple: ((String, String, String, String, String)) => StoredTransformation = {
    case Tuple5(x1, x2, x3, x4, x5) => apply(x1, x2, x3, x4, x5)
  }
}

// After (Scala 3)
object StoredTransformation {
  def fromTuple: ((String, String, String, String, String)) => StoredTransformation = 
    (x1, x2, x3, x4, x5) => apply(x1, x2, x3, x4, x5)
}
```

## Code Changes

### Complete File: SlickStoredTransformations (Scala 2.13 + Slick 3.4)

```scala
package org.mercuree.transformations.core

import slick.jdbc.{JdbcProfile, JdbcBackend}
import JdbcBackend._
import org.slf4j.LoggerFactory
import scala.concurrent.Await
import scala.concurrent.duration._

trait SlickStoredTransformations extends StoredTransformations {

  private final val logger = LoggerFactory.getLogger(getClass)

  val transformationsTableName = "transformations"
  val profile: JdbcProfile = slick.jdbc.H2Profile
  val db: Database

  import profile.api._

  private class TransformationTable(tag: Tag, tableName: String) 
    extends Table[StoredTransformation](tag, tableName) {
    
    def name = column[String]("name", O.PrimaryKey, O.SqlType("varchar(256)"))
    def sqlUpdate = column[String]("update_script", O.SqlType("text"))
    def sqlUpdateHash = column[String]("update_script_hash", O.SqlType("char(128)"))
    def sqlRollback = column[String]("rollback_script", O.SqlType("text"))
    def sqlRollbackHash = column[String]("rollback_script_hash", O.SqlType("char(128)"))
    
    def * = (name, sqlUpdate, sqlUpdateHash, sqlRollback, sqlRollbackHash) <>
      (StoredTransformation.tupled, StoredTransformation.unapply)
  }

  private lazy val storedTransformations = 
    TableQuery[TransformationTable]((tag: Tag) => 
      new TransformationTable(tag, transformationsTableName))

  private def createTransformationTable(): Unit = {
    import slick.jdbc.meta.MTable
    
    val tables = Await.result(db.run(MTable.getTables), Duration.Inf)
    if (!tables.exists(_.name.name == transformationsTableName)) {
      logger.debug(s"Transformation table [$transformationsTableName] is missing!")
      Await.result(db.run(storedTransformations.schema.create), Duration.Inf)
      logger.debug(s"Created transformation table [$transformationsTableName]")
    }
  }

  override def findAllExcept(ids: Set[String]): Seq[StoredTransformation] =
    Await.result(
      db.run(storedTransformations.filterNot(_.name inSet ids).result),
      Duration.Inf
    )

  override def findById(name: String): Option[StoredTransformation] =
    Await.result(
      db.run(storedTransformations.filter(_.name === name).result.headOption),
      Duration.Inf
    )

  override def insert(transformation: LocalTransformation): Unit =
    Await.result(
      db.run(storedTransformations += transformation),
      Duration.Inf
    )

  override def delete(transformation: Transformation): Unit =
    Await.result(
      db.run(storedTransformations.filter(_.name === transformation.id).delete),
      Duration.Inf
    )

  override def update(transformation: LocalTransformation): Unit =
    Await.result(
      db.run(storedTransformations.filter(_.name === transformation.id)
        .update(transformation)),
      Duration.Inf
    )

  override def applyScript(script: String): Unit =
    Await.result(
      db.run(sqlu"#$script"),
      Duration.Inf
    )

  override def transform[A](f: => A): A = {
    createTransformationTable()
    f
  }

  override def transactional[A](f: => A): A =
    Await.result(
      db.run(DBIO.successful(f).transactionally),
      Duration.Inf
    )
}
```

### Complete File: Transformations.scala (with implicit conversion fix)

```scala
// For Scala 2.13
import scala.language.implicitConversions

object LocalTransformation {
  implicit def localToStored(local: LocalTransformation): StoredTransformation = 
    StoredTransformation(
      local.id, 
      local.updateScript, 
      local.updateScriptHash, 
      local.rollbackScript, 
      local.rollbackScriptHash
    )
}

// For Scala 3
object LocalTransformation {
  given Conversion[LocalTransformation, StoredTransformation] with
    def apply(local: LocalTransformation): StoredTransformation = 
      StoredTransformation(
        local.id,
        local.updateScript,
        local.updateScriptHash,
        local.rollbackScript,
        local.rollbackScriptHash
      )
}
```

## Breaking Changes

### Slick 2.0 → 3.x

| Change | Impact | Migration |
|--------|--------|-----------|
| Dynamic sessions removed | HIGH | Use `db.run()` with Await |
| `O.DBType` → `O.SqlType` | MEDIUM | Simple find/replace |
| `simple._` → `api._` | MEDIUM | Update imports |
| Query syntax changes | MEDIUM | Update `.where()` to `.filter()` |
| All operations are Future-based | HIGH | Use Await or async code |

### Scala 2.10 → 2.13

| Change | Impact | Migration |
|--------|--------|-----------|
| XML moved to separate module | MEDIUM | Add scala-xml dependency |
| Some collection methods changed | LOW | Use `.to(List)` instead of `.toList` in some cases |
| Deprecation warnings | LOW | Fix as IDE suggests |

### Scala 2.13 → 3.x

| Change | Impact | Migration |
|--------|--------|-----------|
| `implicit` → `given`/`using` | HIGH | Rewrite implicits |
| Package object changes | LOW | Rarely affects user code |
| Procedure syntax removed | LOW | Add explicit return types |

## Testing Your Migration

### Test Plan

1. **Unit Tests**: Ensure all existing tests pass
2. **Integration Tests**: Test against real databases
3. **Migration Tests**: Apply and rollback transformations
4. **Compatibility Tests**: Test with different databases

### Example Test

```scala
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MigrationCompatibilitySpec extends AnyFlatSpec with Matchers {
  
  "Scala 2.13 migration" should "maintain API compatibility" in {
    val transformation = LocalTransformation(
      id = "test",
      updateScript = "CREATE TABLE test (id INT);",
      rollbackScript = "DROP TABLE test;",
      applyMode = ApplyMode.Once,
      runInTransaction = true
    )
    
    transformation.id shouldBe "test"
    transformation.updateScriptHash should not be empty
  }
  
  "Slick 3.x migration" should "work with H2" in {
    import slick.jdbc.H2Profile.api._
    
    val db = Database.forURL(
      "jdbc:h2:mem:test",
      driver = "org.h2.Driver"
    )
    
    // Should create tables and run migrations
    object TestMigrations extends Transformations 
      with FileLocalTransformations 
      with SlickStoredTransformations {
      
      override val transformationsPath = "/test"
      override val profile = slick.jdbc.H2Profile
      override val db = MigrationCompatibilitySpec.this.db
    }
    
    noException should be thrownBy {
      TestMigrations.run()
    }
    
    db.close()
  }
}
```

## Dependency Updates

### Current Dependencies

```gradle
dependencies {
    compile 'org.scala-lang:scala-library:2.10.3'
    compile 'com.typesafe.slick:slick_2.10:2.0.0'
    compile 'com.h2database:h2:1.3.175'
    compile 'org.slf4j:slf4j-api:1.7.5'
    runtime 'ch.qos.logback:logback-classic:1.0.13'
    testCompile 'junit:junit:4.11'
    testCompile 'org.scalatest:scalatest_2.10:2.0'
    testCompile 'org.scalamock:scalamock-scalatest-support_2.10:3.1.RC1'
}
```

### Updated Dependencies (Scala 2.13)

```gradle
dependencies {
    implementation 'org.scala-lang:scala-library:2.13.12'
    implementation 'com.typesafe.slick:slick_2.13:3.4.1'
    implementation 'com.typesafe.slick:slick-hikaricp_2.13:3.4.1'
    implementation 'org.scala-lang.modules:scala-xml_2.13:2.1.0'
    implementation 'com.h2database:h2:2.2.224'
    implementation 'org.slf4j:slf4j-api:2.0.9'
    runtimeOnly 'ch.qos.logback:logback-classic:1.4.11'
    testImplementation 'org.scalatest:scalatest_2.13:3.2.17'
    testImplementation 'org.scalamock:scalamock_2.13:5.2.0'
}
```

### Updated Dependencies (Scala 3)

```gradle
dependencies {
    implementation 'org.scala-lang:scala3-library_3:3.3.1'
    implementation 'com.typesafe.slick:slick_3:3.4.1'
    implementation 'com.typesafe.slick:slick-hikaricp_3:3.4.1'
    implementation 'org.scala-lang.modules:scala-xml_3:2.1.0'
    implementation 'com.h2database:h2:2.2.224'
    implementation 'org.slf4j:slf4j-api:2.0.9'
    runtimeOnly 'ch.qos.logback:logback-classic:1.4.11'
    testImplementation 'org.scalatest:scalatest_3:3.2.17'
    testImplementation 'org.scalamock:scalamock_3:5.2.0'
}
```

## Build Configuration

### Gradle Wrapper Update

```bash
# Update Gradle wrapper to latest version
./gradlew wrapper --gradle-version 8.5
```

### Complete build.gradle (Scala 2.13)

```gradle
plugins {
    id 'scala'
}

repositories {
    mavenCentral()
}

ext {
    scalaVersion      = "2.13.12"
    scalaMajorVersion = "2.13"
    slickVersion      = "3.4.1"
    h2Version         = "2.2.224"
    slf4jVersion      = "2.0.9"
    logbackVersion    = "1.4.11"
    scalaTestVersion  = "3.2.17"
    scalaMockVersion  = "5.2.0"
}

dependencies {
    implementation "org.scala-lang:scala-library:${scalaVersion}"
    implementation "com.typesafe.slick:slick_${scalaMajorVersion}:${slickVersion}"
    implementation "com.typesafe.slick:slick-hikaricp_${scalaMajorVersion}:${slickVersion}"
    implementation "org.scala-lang.modules:scala-xml_${scalaMajorVersion}:2.1.0"
    implementation "org.slf4j:slf4j-api:${slf4jVersion}"
    implementation "com.h2database:h2:${h2Version}"
    
    runtimeOnly "ch.qos.logback:logback-classic:${logbackVersion}"
    
    testImplementation "org.scalatest:scalatest_${scalaMajorVersion}:${scalaTestVersion}"
    testImplementation "org.scalamock:scalamock_${scalaMajorVersion}:${scalaMockVersion}"
}

sourceCompatibility = JavaVersion.VERSION_1_8
targetCompatibility = JavaVersion.VERSION_1_8

tasks.withType(ScalaCompile) {
    scalaCompileOptions.additionalParameters = [
        "-deprecation",
        "-feature",
        "-unchecked"
    ]
}

test {
    useJUnitPlatform()
}
```

### Complete build.sbt (Scala 2.13)

```scala
lazy val root = (project in file("."))
  .settings(
    name := "transformations-core",
    organization := "org.mercuree",
    version := "1.0.0",
    scalaVersion := "2.13.12",
    
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint"
    ),
    
    libraryDependencies ++= Seq(
      "com.typesafe.slick" %% "slick" % "3.4.1",
      "com.typesafe.slick" %% "slick-hikaricp" % "3.4.1",
      "org.scala-lang.modules" %% "scala-xml" % "2.1.0",
      "com.h2database" % "h2" % "2.2.224",
      "org.slf4j" % "slf4j-api" % "2.0.9",
      "ch.qos.logback" % "logback-classic" % "1.4.11" % Runtime,
      
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.scalamock" %% "scalamock" % "5.2.0" % Test
    )
  )
```

## Migration Checklist

### Pre-Migration

- [ ] Backup your codebase
- [ ] Document current versions
- [ ] Run all tests and ensure they pass
- [ ] Create a migration branch

### Scala 2.13 Migration

- [ ] Update `build.gradle` or `build.sbt`
  - [ ] Update Scala version to 2.13.x
  - [ ] Update Slick to 3.4.x
  - [ ] Add scala-xml dependency
  - [ ] Add slick-hikaricp dependency
  - [ ] Update test dependencies
- [ ] Update imports
  - [ ] Change `scala.slick.*` to `slick.*`
  - [ ] Change `profile.simple._` to `profile.api._`
- [ ] Update Slick code
  - [ ] Replace dynamic sessions with `db.run()`
  - [ ] Add Await calls for synchronous behavior
  - [ ] Change `O.DBType` to `O.SqlType`
  - [ ] Update query syntax (`.where()` → `.filter()`)
  - [ ] Update table projections (`<>` usage)
- [ ] Update SQL execution
  - [ ] Replace `StaticQuery` with `sqlu` interpolator
- [ ] Run tests
  - [ ] Fix any compilation errors
  - [ ] Fix any runtime errors
  - [ ] Verify all tests pass

### Scala 3 Migration (Optional)

- [ ] Update Scala version to 3.3.x
- [ ] Update implicit conversions to given/using
- [ ] Update pattern matching where needed
- [ ] Fix any Scala 3 specific issues
- [ ] Run tests

### Post-Migration

- [ ] Run full test suite
- [ ] Test against all target databases
- [ ] Review performance
- [ ] Update documentation
- [ ] Merge migration branch

## Troubleshooting

### Issue: Compilation errors with Slick imports

**Solution**: Ensure you're using the right import path
```scala
// Correct for Slick 3
import slick.jdbc.H2Profile.api._

// Not this (Slick 2)
import scala.slick.driver.H2Driver.simple._
```

### Issue: Runtime errors with "No implicit session"

**Solution**: Slick 3 doesn't use implicit sessions. Use `db.run()`:
```scala
// Don't do this
storedTransformations.list  // Slick 2 style

// Do this
Await.result(db.run(storedTransformations.result), Duration.Inf)
```

### Issue: XML parsing errors

**Solution**: Add scala-xml dependency:
```gradle
implementation 'org.scala-lang.modules:scala-xml_2.13:2.1.0'
```

### Issue: Tests fail with async timeouts

**Solution**: Increase timeout or use proper async testing:
```scala
import scala.concurrent.duration._

Await.result(future, 30.seconds)  // Increase timeout
```

---

For questions or issues with migration:
- Open an issue on [GitHub](https://github.com/zeddius1983/transformations/issues)
- Consult [DOCUMENTATION.md](DOCUMENTATION.md) for API details
- See [EXAMPLES.md](EXAMPLES.md) for updated code examples
