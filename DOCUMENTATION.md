# Technical Documentation

Comprehensive technical documentation for the Transformations Framework.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Core Components](#core-components)
- [Transformation Lifecycle](#transformation-lifecycle)
- [Advanced Usage](#advanced-usage)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [API Reference](#api-reference)
- [Performance Considerations](#performance-considerations)

## Architecture Overview

The Transformations Framework uses a **trait-based architecture** that provides flexibility through composition. This design allows you to mix different implementations to create a customized solution for your needs.

### High-Level Architecture

```
┌────────────────────────────────────────────────────────────┐
│                    Application Layer                       │
│  (Your code combining traits to build migration system)   │
└────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌───────────────┐  ┌──────────────────┐  ┌──────────────────┐
│Transformations│  │LocalTransform-   │  │StoredTransform-  │
│               │  │ations            │  │ations            │
│ Core Logic:   │  │                  │  │                  │
│ • Apply       │  │ Loading:         │  │ Persistence:     │
│ • Rollback    │  │ • Find files     │  │ • CRUD ops       │
│ • Ordering    │  │ • Parse SQL/XML  │  │ • Hashing        │
│ • Hooks       │  │ • Version sort   │  │ • DB execution   │
└───────────────┘  └──────────────────┘  └──────────────────┘
                            │                   │
                ┌───────────┴────┐   ┌──────────┴─────────┐
                ▼                ▼   ▼                    ▼
        ┌─────────────┐  ┌─────────────┐        ┌────────────────┐
        │FileLocal    │  │URLLocal     │        │SlickStored     │
        │Transform-   │  │Transform-   │        │Transform-      │
        │ations       │  │ations       │        │ations          │
        │             │  │             │        │                │
        │ File system │  │ Resources   │        │ Slick/JDBC     │
        │ Semantic    │  │ Classpath   │        │ SQL queries    │
        │ versioning  │  │ JAR support │        │ Transactions   │
        └─────────────┘  └─────────────┘        └────────────────┘
```

### Design Principles

1. **Separation of Concerns**: Each trait has a single, well-defined responsibility
2. **Composition over Inheritance**: Build solutions by mixing traits
3. **Open/Closed Principle**: Extend behavior without modifying core code
4. **Dependency Inversion**: Depend on abstractions, not concrete implementations

### Key Design Patterns

- **Strategy Pattern**: Different apply modes (Once, Modified, Always)
- **Template Method**: Transformation lifecycle with customizable hooks
- **Facade Pattern**: Simple API hiding complex transformation logic
- **Repository Pattern**: StoredTransformations abstracts persistence

## Core Components

### 1. Transformation Hierarchy

The framework defines several transformation types:

```scala
// Base trait - all transformations have an ID
trait Transformation {
  val id: String
}

// Transformations with scripts and hash checksums
trait ScriptedTransformation extends Transformation {
  val updateScript: String
  val updateScriptHash: String
  val rollbackScript: String
  val rollbackScriptHash: String
}

// A transformation to be applied
case class LocalTransformation(
  id: String,
  updateScript: String,
  rollbackScript: String,
  applyMode: ApplyMode.Value,
  runInTransaction: Boolean
) extends ScriptedTransformation {
  // Hash computed via MD5
  val updateScriptHash = md5(updateScript)
  val rollbackScriptHash = md5(rollbackScript)
}

// A transformation already applied to database
case class StoredTransformation(
  id: String,
  updateScript: String,
  updateScriptHash: String,
  rollbackScript: String,
  rollbackScriptHash: String
) extends ScriptedTransformation

// Special transformation types
case class SkippedTransformation(id: String) extends Transformation
case class DisabledTransformation(id: String) extends Transformation
```

**Usage Example**:

```scala
// Create a transformation programmatically
val transformation = LocalTransformation(
  id = "1.0.0/create_users.sql",
  updateScript = "CREATE TABLE users (id BIGINT PRIMARY KEY);",
  rollbackScript = "DROP TABLE users;",
  applyMode = ApplyMode.Once,
  runInTransaction = true
)

// Access computed hash
println(transformation.updateScriptHash) // MD5 hash of update script
```

### 2. Apply Modes

The `ApplyMode` enumeration controls when transformations execute:

```scala
object ApplyMode extends Enumeration {
  val Once = Value      // Run only if not yet applied
  val Modified = Value  // Re-run when update script changes
  val Always = Value    // Run every time
}
```

#### Decision Matrix

The framework uses this logic to determine if a transformation should run:

```
┌─────────────────────────────────────────────────────────┐
│ Transformation Application Decision Tree                │
└─────────────────────────────────────────────────────────┘

Is transformation in database?
│
├─ NO ──────────────────┐
│                       │
│                       ▼
│               ┌───────────────┐
│               │ Apply Update  │
│               │ Insert Record │
│               └───────────────┘
│
└─ YES ─────────────────┐
                        │
                        ▼
                Check Apply Mode
                │
                ├─ Once ─────────────────┐
                │                        │
                │                        ▼
                │                ┌──────────────┐
                │                │ Skip (Done)  │
                │                └──────────────┘
                │
                ├─ Modified ─────────────┐
                │                        │
                │                        ▼
                │               Compare Hash
                │               │
                │               ├─ Same ──────────────┐
                │               │                     │
                │               │                     ▼
                │               │             ┌──────────────┐
                │               │             │ Skip (Same)  │
                │               │             └──────────────┘
                │               │
                │               └─ Different ─────────┐
                │                                     │
                │                                     ▼
                │                           ┌──────────────────┐
                │                           │ Rollback + Apply │
                │                           │ Update Record    │
                │                           └──────────────────┘
                │
                └─ Always ───────────────────┐
                                             │
                                             ▼
                                    ┌──────────────────┐
                                    │ Rollback + Apply │
                                    │ Update Record    │
                                    └──────────────────┘
```

**Hash Comparison Details**:

The framework computes MD5 hashes to detect changes:

```scala
private def md5(text: String) = 
  MessageDigest.getInstance("MD5")
    .digest(text.getBytes)
    .map("%02x".format(_))
    .mkString
```

If only the **rollback script** changes (update hash same):
- The transformation is **not re-applied**
- Only the stored record is **updated** with new rollback hash

### 3. LocalTransformations Trait

Provides transformations to be applied.

```scala
trait LocalTransformations {
  /**
   * Returns transformations in the order they should be applied.
   * Ordering is guaranteed to be preserved.
   */
  def localTransformations: List[Transformation]
}
```

### 4. FileLocalTransformations Implementation

Loads transformations from the file system with semantic version ordering.

#### Algorithm

```
1. Get root path (from classpath or file system)
2. Recursively scan for .sql and .xml files
3. Parse each file into a Transformation
4. Sort using FilePathOrdering:
   a. Split paths into components
   b. Extract version numbers (e.g., "1.2.0")
   c. Compare using semantic versioning
   d. Fall back to lexicographic for non-versions
5. Return sorted list
```

#### Semantic Version Ordering

```scala
object FilePathOrdering extends Ordering[String] {
  private val Version = """(\d+)(\.\d+)*""".r
  
  override def compare(left: String, right: String): Int = {
    // Split into path components
    val leftParts = left.split(File.separator).toList
    val rightParts = right.split(File.separator).toList
    
    // Compare each component
    comparePathElements(leftParts, rightParts)
  }
  
  private def comparePathElement(left: String, right: String): Int = {
    // Try to extract version numbers
    val leftVersionOption = Version.findFirstIn(left)
    val rightVersionOption = Version.findFirstIn(right)
    
    (leftVersionOption, rightVersionOption) match {
      case (Some(lv), Some(rv)) => 
        compareVersions(lv, rv)  // Numeric comparison
      case (Some(_), None) => -1  // Version comes before non-version
      case (None, Some(_)) => 1
      case (None, None) => left.compareTo(right)  // Lexicographic
    }
  }
}
```

**Ordering Examples**:

```
Input files:
- 2.0/add.sql
- 1.0/create.sql
- 10.0/update.sql
- 1.1/patch.sql

Sorted order:
1. 1.0/create.sql
2. 1.1/patch.sql
3. 2.0/add.sql
4. 10.0/update.sql    <- Note: 10 comes after 2 (numeric, not lexicographic)
```

#### File Naming Conventions

```
Recognized patterns:
✓ file.sql           - Applied
✓ file.xml           - Applied
✓ -file.sql          - Skipped (prefix with -)
✗ file.txt           - Ignored
✗ file.sql.bak       - Ignored
```

### 5. StoredTransformations Trait

Manages persistence of applied transformations.

```scala
trait StoredTransformations {
  // Query operations
  def findAllExcept(ids: Set[String]): Seq[StoredTransformation]
  def findById(id: String): Option[StoredTransformation]
  
  // Modification operations
  def insert(transformation: LocalTransformation): Unit
  def delete(transformation: Transformation): Unit
  def update(transformation: LocalTransformation): Unit
  
  // Script execution
  def applyScript(script: String): Unit
  
  // Lifecycle hooks
  def transform[A](f: => A): A        // Wrapper for entire run
  def transactional[A](f: => A): A    // Transaction boundary
}
```

### 6. SlickStoredTransformations Implementation

Slick-based implementation using JDBC.

#### Database Schema

```sql
CREATE TABLE transformations (
    name VARCHAR(256) PRIMARY KEY,
    update_script TEXT NOT NULL,
    update_script_hash CHAR(128) NOT NULL,
    rollback_script TEXT NOT NULL,
    rollback_script_hash CHAR(128) NOT NULL
);
```

**Note**: The table is created automatically if it doesn't exist.

#### Implementation Details

```scala
trait SlickStoredTransformations extends StoredTransformations {
  val transformationsTableName = "transformations"
  val profile: JdbcProfile = GenericDriver
  val db: Database
  
  import profile.simple._
  
  // Table definition
  private class TransformationTable(tag: Tag, tableName: String) 
    extends Table[StoredTransformation](tag, tableName) {
    
    def name = column[String]("name", O.PrimaryKey, O.DBType("varchar(256)"))
    def sqlUpdate = column[String]("update_script", O.DBType("text"))
    def sqlUpdateHash = column[String]("update_script_hash", O.DBType("char(128)"))
    def sqlRollback = column[String]("rollback_script", O.DBType("text"))
    def sqlRollbackHash = column[String]("rollback_script_hash", O.DBType("char(128)"))
    
    def * = (name, sqlUpdate, sqlUpdateHash, sqlRollback, sqlRollbackHash) <> 
      (StoredTransformation.fromTuple, StoredTransformation.unapply)
  }
  
  // Auto-create table if missing
  private def createTransformationTable(): Unit = db.withDynSession {
    import scala.slick.jdbc.meta.MTable
    if (!MTable.getTables.list.exists(_.name.name == transformationsTableName)) {
      storedTransformations.ddl.create
    }
  }
}
```

#### Customization

```scala
// Custom table name
trait CustomStoredTransformations extends SlickStoredTransformations {
  override val transformationsTableName = "schema_migrations"
}

// Different database driver
trait PostgresStoredTransformations extends SlickStoredTransformations {
  override val profile = scala.slick.driver.PostgresDriver
}
```

## Transformation Lifecycle

### Apply Logic Flowchart

```
┌─────────────┐
│ Start Run   │
└──────┬──────┘
       │
       ▼
┌──────────────────────────┐
│ Load Local               │
│ Transformations          │
│ (from files/resources)   │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────┐
│ Create Transformations   │
│ Table (if not exists)    │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────┐
│ For Each Local           │
│ Transformation:          │
└──────┬───────────────────┘
       │
       ├───── SkippedTransformation ────> Log "Skipped"
       │
       ├───── DisabledTransformation ───> Rollback if exists
       │
       └───── LocalTransformation ──────┐
                                        │
                                        ▼
                              ┌─────────────────┐
                              │ Find in DB      │
                              └────────┬────────┘
                                       │
                        ┌──────────────┼──────────────┐
                        │                             │
                        ▼                             ▼
                  ┌──────────┐               ┌────────────────┐
                  │ Not Found│               │ Found          │
                  └────┬─────┘               └───────┬────────┘
                       │                             │
                       ▼                             ▼
              ┌──────────────────┐         ┌──────────────────┐
              │ Apply Update     │         │ Check Apply Mode │
              │ Insert Record    │         └────────┬─────────┘
              └──────────────────┘                  │
                                        ┌───────────┼───────────┐
                                        │           │           │
                                        ▼           ▼           ▼
                                    ┌──────┐  ┌─────────┐  ┌────────┐
                                    │ Once │  │Modified │  │ Always │
                                    └──┬───┘  └────┬────┘  └────┬───┘
                                       │           │            │
                                       ▼           ▼            │
                                    ┌──────┐  ┌─────────┐      │
                                    │ Skip │  │Compare  │      │
                                    └──────┘  │  Hash   │      │
                                              └────┬────┘      │
                                               ┌───┴───┐       │
                                               ▼       ▼       │
                                           ┌──────┐ ┌────┐    │
                                           │ Same │ │Diff│    │
                                           └──────┘ └─┬──┘    │
                                                      │        │
                                                      ▼        ▼
                                              ┌────────────────────┐
                                              │ Rollback + Apply   │
                                              │ Update Record      │
                                              └────────────────────┘
┌──────────────────────────┐
│ Find Stored              │
│ Transformations NOT in   │
│ Local (removed files)    │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────┐
│ For Each Orphaned:       │
│ Rollback + Delete        │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────┐
│ End Run                  │
└──────────────────────────┘
```

### Rollback Logic Flowchart

```
┌──────────────────────┐
│ Rollback Requested   │
│ (disabled or removed)│
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ Find in Database     │
└──────────┬───────────┘
           │
     ┌─────┴─────┐
     │           │
     ▼           ▼
┌─────────┐  ┌──────────┐
│ Found   │  │Not Found │
└────┬────┘  └────┬─────┘
     │            │
     │            ▼
     │       ┌──────────┐
     │       │ Skip     │
     │       │ (nothing │
     │       │ to do)   │
     │       └──────────┘
     │
     ▼
┌──────────────────────┐
│ Execute Rollback     │
│ Script               │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ Delete from Database │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ Log Success          │
└──────────────────────┘
```

## Advanced Usage

### Custom LocalTransformations Implementation

Create a custom source for transformations:

```scala
// Load transformations from a database
trait DatabaseLocalTransformations extends LocalTransformations {
  val configDb: Database
  
  def localTransformations: List[Transformation] = {
    configDb.withSession { implicit session =>
      sql"""
        SELECT id, update_sql, rollback_sql, apply_mode
        FROM migration_config
        ORDER BY sequence
      """.as[(String, String, String, String)].list.map {
        case (id, update, rollback, mode) =>
          LocalTransformation(
            id = id,
            updateScript = update,
            rollbackScript = rollback,
            applyMode = ApplyMode.withName(mode),
            runInTransaction = true
          )
      }
    }
  }
}

// Load from REST API
trait RemoteLocalTransformations extends LocalTransformations {
  val apiUrl: String
  
  def localTransformations: List[Transformation] = {
    val response = scala.io.Source.fromURL(apiUrl).mkString
    // Parse JSON and convert to transformations
    parseJsonToTransformations(response)
  }
  
  private def parseJsonToTransformations(json: String): List[Transformation] = {
    // Implementation...
  }
}
```

### Custom StoredTransformations Implementation

Implement custom persistence:

```scala
// MongoDB-based storage
trait MongoStoredTransformations extends StoredTransformations {
  val mongoClient: MongoClient
  val collection: MongoCollection
  
  override def findById(id: String): Option[StoredTransformation] = {
    collection.find(equal("_id", id)).first() match {
      case null => None
      case doc => Some(documentToTransformation(doc))
    }
  }
  
  override def insert(transformation: LocalTransformation): Unit = {
    val doc = new Document()
      .append("_id", transformation.id)
      .append("updateScript", transformation.updateScript)
      .append("updateScriptHash", transformation.updateScriptHash)
      .append("rollbackScript", transformation.rollbackScript)
      .append("rollbackScriptHash", transformation.rollbackScriptHash)
      .append("appliedAt", new Date())
    
    collection.insertOne(doc)
  }
  
  // Implement other methods...
}
```

### Multiple Database Support

Support different databases in one application:

```scala
class MultiDatabaseMigrations {
  
  // Primary database transformations
  object PrimaryMigrations extends Transformations
    with FileLocalTransformations
    with SlickStoredTransformations {
    
    override val transformationsPath = "/migrations/primary"
    override val db = Database.forURL("jdbc:postgresql://localhost/primary")
    override val profile = scala.slick.driver.PostgresDriver
  }
  
  // Analytics database transformations
  object AnalyticsMigrations extends Transformations
    with FileLocalTransformations
    with SlickStoredTransformations {
    
    override val transformationsPath = "/migrations/analytics"
    override val db = Database.forURL("jdbc:mysql://localhost/analytics")
    override val profile = scala.slick.driver.MySQLDriver
  }
  
  def runAll(): Unit = {
    println("Migrating primary database...")
    PrimaryMigrations.run
    
    println("Migrating analytics database...")
    AnalyticsMigrations.run
  }
}
```

### Conditional Transformations

Apply transformations based on conditions:

```scala
trait ConditionalTransformations extends Transformations 
  with LocalTransformations
  with StoredTransformations {
  
  val environment: String // "dev", "staging", "prod"
  
  abstract override def localTransformations: List[Transformation] = {
    super.localTransformations.filter { t =>
      // Only apply dev-specific migrations in dev
      if (t.id.contains("/dev/")) {
        environment == "dev"
      }
      // Only apply prod migrations in prod
      else if (t.id.contains("/prod/")) {
        environment == "prod"
      }
      // Apply all others
      else {
        true
      }
    }
  }
}
```

### Pre/Post Processing Hooks

Add custom logic around transformations:

```scala
trait MonitoredTransformations extends Transformations {
  
  val metrics: MetricsService
  val notifications: NotificationService
  
  override protected def onApply(local: LocalTransformation): Unit = {
    val startTime = System.currentTimeMillis()
    
    try {
      // Pre-apply hook
      notifications.send(s"Starting migration: ${local.id}")
      
      // Execute transformation
      super.onApply(local)
      
      // Post-apply success hook
      val duration = System.currentTimeMillis() - startTime
      metrics.recordSuccess(local.id, duration)
      notifications.send(s"Completed migration: ${local.id} in ${duration}ms")
      
    } catch {
      case e: Exception =>
        // Post-apply failure hook
        val duration = System.currentTimeMillis() - startTime
        metrics.recordFailure(local.id, duration, e)
        notifications.sendError(s"Failed migration: ${local.id}", e)
        throw e
    }
  }
  
  override protected def onRollback(transformation: Transformation): Unit = {
    // Pre-rollback hook
    notifications.send(s"Rolling back: ${transformation.id}")
    
    // Execute rollback
    super.onRollback(transformation)
    
    // Post-rollback hook
    metrics.recordRollback(transformation.id)
  }
}
```

## Best Practices

### File Organization

#### Recommended Structure

```
transformations/
├── 1.0.0/              # Major version: Initial release
│   ├── 001_schema.sql  # Numbered for ordering
│   ├── 002_users.sql
│   └── 003_products.sql
│
├── 1.1.0/              # Minor version: New features
│   ├── 001_add_categories.sql
│   └── 002_user_preferences.sql
│
├── 1.1.1/              # Patch version: Fixes
│   └── 001_fix_user_index.sql
│
├── 2.0.0/              # Major version: Breaking changes
│   ├── 001_refactor_schema.sql
│   └── 002_migrate_data.sql
│
└── dev/                # Development-only (conditionally applied)
    └── 001_test_data.sql
```

#### Naming Conventions

**DO**:
```
✓ 001_create_users_table.sql
✓ 002_add_email_index.sql
✓ 003_create_products_table.sql
✓ 1.0.0/initial_schema.sql
✓ 2.0.0/refactor_users.sql
```

**DON'T**:
```
✗ createUsers.sql              # Not numbered
✗ fix.sql                      # Not descriptive
✗ 2020-01-01-schema.sql        # Date-based (use versions)
✗ migration1.sql               # Generic name
```

### Safe Transformation Writing

#### DO: Write Idempotent Scripts

```sql
--<transformation apply="once">
--<update>
-- Always use IF NOT EXISTS
CREATE TABLE IF NOT EXISTS users (
    id BIGINT PRIMARY KEY
);

-- Always use IF EXISTS for drops
ALTER TABLE users DROP COLUMN IF EXISTS old_column;

-- Check before adding constraints
-- (Database-specific syntax)
--</update>
--</transformation>
```

#### DON'T: Assume Clean State

```sql
-- BAD: Assumes table doesn't exist
CREATE TABLE users (id BIGINT);

-- BAD: Assumes column exists
ALTER TABLE users DROP COLUMN old_column;
```

#### DO: Order Foreign Keys Correctly

```sql
--<update>
-- Create parent tables first
CREATE TABLE users (id BIGINT PRIMARY KEY);
CREATE TABLE products (id BIGINT PRIMARY KEY);

-- Then child tables
CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    user_id BIGINT,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
--</update>

--<rollback>
-- Drop in REVERSE order
DROP TABLE orders;
DROP TABLE products;
DROP TABLE users;
--</rollback>
```

#### DO: Handle Data Safely

```sql
--<update>
-- Add column with default
ALTER TABLE users ADD COLUMN status VARCHAR(20) DEFAULT 'active';

-- Update existing rows (if needed)
UPDATE users SET status = 'active' WHERE status IS NULL;

-- Add NOT NULL constraint after data is populated
ALTER TABLE users MODIFY COLUMN status VARCHAR(20) NOT NULL;
--</update>
```

### Transaction Strategies

#### When to Use Transactions

```sql
-- YES: DDL changes (most databases)
--<transformation transaction="true">
--<update>
CREATE TABLE users (...);
ALTER TABLE users ADD COLUMN ...;
--</update>
--</transformation>

-- YES: Data changes
--<transformation transaction="true">
--<update>
UPDATE users SET status = 'migrated';
INSERT INTO audit_log ...;
--</update>
--</transformation>

-- NO: Operations that can't be transactional
--<transformation transaction="false">
--<update>
-- Some databases don't support DDL in transactions
-- Or for very large data migrations that might timeout
--</update>
--</transformation>
```

#### Database-Specific Considerations

**PostgreSQL**: Supports DDL in transactions
```sql
--<transformation transaction="true">
```

**MySQL**: DDL causes implicit commit
```sql
--<transformation transaction="false">
```

### Rollback Script Guidelines

#### DO: Write Complete Rollbacks

```sql
--<transformation>
--<update>
CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    username VARCHAR(100),
    email VARCHAR(255)
);
CREATE INDEX idx_users_email ON users(email);
INSERT INTO users (username, email) VALUES ('admin', 'admin@example.com');
--</update>

--<rollback>
-- Reverse ALL changes made in update
DELETE FROM users WHERE username = 'admin';
DROP INDEX idx_users_email;
DROP TABLE users;
--</rollback>
--</transformation>
```

#### DO: Test Rollbacks

```scala
// Test that rollback actually works
"Migration" should "be fully reversible" in {
  // Apply
  migration.run()
  assert(tableExists("users"))
  
  // Rollback
  migration.rollback()
  assert(!tableExists("users"))
  
  // Re-apply
  migration.run()
  assert(tableExists("users"))
}
```

### Views and Stored Procedures

Use `apply="modified"` for database objects that should be updated:

```sql
--<transformation apply="modified">
--<update>
CREATE OR REPLACE VIEW active_users AS
SELECT u.*
FROM users u
WHERE u.deleted_at IS NULL
  AND u.status = 'active';
--</update>

--<rollback>
DROP VIEW IF EXISTS active_users;
--</rollback>
--</transformation>
```

### Testing Transformations

#### Unit Tests

```scala
class TransformationParsingSpec extends FlatSpec {
  "SQL transformation" should "parse correctly" in {
    val sql = """
      |--<transformation>
      |--<update>
      |CREATE TABLE test (id INT);
      |--</update>
      |--<rollback>
      |DROP TABLE test;
      |--</rollback>
      |--</transformation>
    """.stripMargin
    
    val t = LocalTransformation.parseSQL(sql, "test.sql")
    
    assert(t.isInstanceOf[LocalTransformation])
    assert(t.asInstanceOf[LocalTransformation].updateScript.contains("CREATE TABLE"))
  }
}
```

#### Integration Tests

```scala
class MigrationIntegrationSpec extends FlatSpec {
  val db = Database.forURL("jdbc:h2:mem:test")
  
  "Migrations" should "apply successfully" in {
    object TestMigrations extends Transformations
      with FileLocalTransformations
      with SlickStoredTransformations {
      
      val transformationsPath = "/test/migrations"
      val profile = H2Driver
    }
    
    // Should not throw
    TestMigrations.run()
    
    // Verify results
    db.withSession { implicit session =>
      val tables = sql"SHOW TABLES".as[String].list
      assert(tables.contains("users"))
    }
  }
}
```

## Troubleshooting

### Common Issues and Solutions

#### Issue: Transformation table not created

**Symptoms**: Errors about missing `transformations` table

**Cause**: Database user lacks CREATE TABLE permissions

**Solution**:
```sql
-- Grant necessary permissions
GRANT CREATE, ALTER, DROP ON DATABASE mydb TO myuser;
```

#### Issue: Hash mismatch on every run

**Symptoms**: Transformations with `apply="modified"` re-run every time

**Cause**: Whitespace differences in script text

**Solution**: Use consistent whitespace:
```scala
// Trim scripts before hashing
val normalizedScript = updateScript.trim
```

#### Issue: Foreign key constraint errors during rollback

**Symptoms**: Cannot drop tables due to foreign keys

**Solution**: Use correct drop order or disable checks:
```sql
--<rollback>
-- Option 1: Drop in reverse order
DROP TABLE child_table;
DROP TABLE parent_table;

-- Option 2: Temporarily disable FK checks (MySQL)
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE parent_table;
DROP TABLE child_table;
SET FOREIGN_KEY_CHECKS = 1;
--</rollback>
```

#### Issue: Transaction timeout on large migrations

**Symptoms**: Transaction timeouts or database locks

**Solution**: Disable transactions for large operations:
```sql
--<transformation transaction="false">
--<update>
-- Large data migration
INSERT INTO new_table SELECT * FROM old_table;
--</update>
--</transformation>
```

### Debug Logging

Enable detailed logging:

```scala
// In logback.xml
<logger name="org.mercuree.transformations" level="DEBUG"/>

// Or programmatically
import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory

val root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
root.setLevel(Level.DEBUG)
```

### Manual Recovery

If migrations fail mid-process:

```sql
-- 1. Check what was applied
SELECT * FROM transformations ORDER BY name;

-- 2. Manually rollback if needed
-- Execute the rollback script from the file

-- 3. Remove from tracking table
DELETE FROM transformations WHERE name = '1.0.0/problem_migration.sql';

-- 4. Fix the transformation file

-- 5. Re-run migrations
```

## API Reference

### Transformation Types

#### `trait Transformation`
Base type for all transformations.

**Properties**:
- `id: String` - Unique identifier (usually file path)

#### `trait ScriptedTransformation extends Transformation`
Transformation with SQL scripts and hashes.

**Properties**:
- `updateScript: String` - SQL to apply
- `updateScriptHash: String` - MD5 hash of update script
- `rollbackScript: String` - SQL to rollback
- `rollbackScriptHash: String` - MD5 hash of rollback script

#### `case class LocalTransformation(...)`
A transformation to be applied.

**Constructor**:
```scala
LocalTransformation(
  id: String,
  updateScript: String,
  rollbackScript: String,
  applyMode: ApplyMode.Value,
  runInTransaction: Boolean
)
```

**Methods**:
- `LocalTransformation.fromFile(file: File, id: String): Transformation`
- `LocalTransformation.fromURL(url: URL, id: String): Transformation`
- `LocalTransformation.parseSQL(sql: String, id: String): Transformation`
- `LocalTransformation.parseXML(xml: Elem, id: String): Transformation`

#### `case class StoredTransformation(...)`
A transformation that has been applied.

**Constructor**:
```scala
StoredTransformation(
  id: String,
  updateScript: String,
  updateScriptHash: String,
  rollbackScript: String,
  rollbackScriptHash: String
)
```

#### `case class SkippedTransformation(id: String)`
Transformation marked to be skipped (file prefixed with `-`).

#### `case class DisabledTransformation(id: String)`
Transformation marked as disabled (`enabled="false"`).

### Traits

#### `trait LocalTransformations`

**Methods**:
```scala
def localTransformations: List[Transformation]
```
Returns ordered list of transformations to apply.

#### `trait FileLocalTransformations extends LocalTransformations`

**Properties**:
```scala
val transformationsPath: String
```
Path to transformations directory (relative to classpath or absolute).

**Protected**:
```scala
protected val FilePattern: Regex
```
Pattern for matching transformation files (default: `.sql` or `.xml`).

#### `trait StoredTransformations`

**Methods**:
```scala
def findAllExcept(ids: Set[String]): Seq[StoredTransformation]
def findById(id: String): Option[StoredTransformation]
def insert(transformation: LocalTransformation): Unit
def delete(transformation: Transformation): Unit
def update(transformation: LocalTransformation): Unit
def applyScript(script: String): Unit
def transform[A](f: => A): A
def transactional[A](f: => A): A
```

#### `trait SlickStoredTransformations extends StoredTransformations`

**Properties**:
```scala
val transformationsTableName: String  // Default: "transformations"
val profile: JdbcProfile              // Database driver
val db: Database                      // Database connection
```

#### `trait Transformations`

**Methods**:
```scala
def run: Unit
```
Main entry point - applies all transformations.

**Protected (for overriding)**:
```scala
protected def onApply(local: LocalTransformation): Unit
protected def onRollback(transformation: Transformation): Unit
protected def tryApply(local: LocalTransformation): Unit
protected def tryRollback(transformation: Transformation): Unit
```

### Enumerations

#### `object ApplyMode`

**Values**:
- `ApplyMode.Once` - Apply only if not yet applied
- `ApplyMode.Modified` - Re-apply when update script changes
- `ApplyMode.Always` - Apply every time

**Methods**:
```scala
ApplyMode.withName(name: String): ApplyMode.Value
```

### Exceptions

#### `case class TransformationException(message: String)`
Thrown when transformation parsing or application fails.

## Performance Considerations

### File Scanning

**Issue**: Large transformation directories can slow startup

**Optimization**:
```scala
// Cache file list if running multiple times
trait CachedFileLocalTransformations extends FileLocalTransformations {
  private lazy val cached = super.localTransformations
  override def localTransformations = cached
}
```

### Database Queries

**Issue**: N+1 queries when checking transformations

**Current**: Already optimized - uses `findAllExcept` with single query

### Hash Computation

**Issue**: MD5 computation on every run

**Note**: Minimal overhead; MD5 is very fast. Hashes are not recomputed for unchanged transformations.

### Transaction Overhead

**Issue**: Large transactions can lock tables

**Solution**: Disable transactions for large migrations:
```sql
--<transformation transaction="false">
```

Or batch operations:
```sql
--<update>
-- Process in batches
UPDATE users SET migrated = TRUE WHERE id BETWEEN 1 AND 1000;
-- Split into multiple transformations if needed
--</update>
```

### Parallel Execution

**Current Limitation**: Transformations run sequentially

**Future**: Parallel execution for independent transformations (planned)

**Workaround**: Run multiple migration instances for different schemas:
```scala
// Can run in parallel
Future { PrimaryMigrations.run }
Future { AnalyticsMigrations.run }
```

---

For more information, see:
- [README.md](README.md) - Overview and features
- [QUICKSTART.md](QUICKSTART.md) - Getting started guide
- [EXAMPLES.md](EXAMPLES.md) - Practical examples
- [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) - Upgrading guide
