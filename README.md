# Transformations Framework

> A powerful, version-controlled database migration framework for Scala applications

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

The Transformations Framework provides a robust, easy-to-use solution for tracking, managing, and applying database schema changes with full version control and rollback capabilities.

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Basic Usage](#basic-usage)
- [Transformation Formats](#transformation-formats)
- [Apply Modes](#apply-modes)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Advanced Features](#advanced-features)
- [Testing](#testing)
- [Requirements](#requirements)
- [Contributing](#contributing)
- [Roadmap](#roadmap)
- [Documentation](#documentation)
- [License](#license)

## Features

✨ **Version Control** - Track all database changes with semantic versioning  
🔄 **Rollback Support** - Safely revert changes with automatic rollback scripts  
📁 **File-based Migrations** - Store transformations as SQL or XML files  
🎯 **Smart Apply Modes** - Control when transformations run (Once, Modified, Always)  
🗄️ **Multiple Formats** - Support for both SQL and XML transformation formats  
🔒 **Transaction Safety** - Optional transactional execution for data integrity  
🎨 **Flexible Architecture** - Trait-based design for easy customization  
📊 **Persistence Layer** - Track applied transformations in your database  
🧪 **Test-friendly** - Comprehensive test support with mock implementations

## Quick Start

### 1. Add Dependency

Add to your `build.gradle`:

```gradle
dependencies {
    compile 'org.mercuree:transformations-core:1.0.0'
}
```

### 2. Create Your First Transformation

Create a file `transformations/1.0.0/create_users_table.sql`:

```sql
--<transformation>
--<update>
CREATE TABLE users (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
--</update>

--<rollback>
DROP TABLE users;
--</rollback>
--</transformation>
```

### 3. Run Migrations

```scala
import org.mercuree.transformations.core._
import scala.slick.jdbc.JdbcBackend.Database

object MigrationRunner extends App {
  val db = Database.forURL(
    "jdbc:h2:mem:test",
    driver = "org.h2.Driver"
  )
  
  val migrations = new Transformations 
    with FileLocalTransformations 
    with SlickStoredTransformations {
    
    val transformationsPath = "transformations"
    val profile = scala.slick.driver.H2Driver
  }
  
  migrations.run
}
```

That's it! Your database is now migrated. See [QUICKSTART.md](QUICKSTART.md) for a detailed walkthrough.

## Installation

### Gradle

```gradle
repositories {
    mavenCentral()
}

dependencies {
    compile 'org.mercuree:transformations-core:1.0.0'
    compile 'com.typesafe.slick:slick_2.10:2.0.0'
    compile 'com.h2database:h2:1.3.175' // or your database driver
}
```

### Requirements

- Scala 2.10+ (see [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for upgrading)
- Java 6+
- Slick 2.0+ for database access
- JDBC-compatible database

## Basic Usage

### Creating Transformations

Transformations can be written in two formats:

#### SQL Format (Recommended)

```sql
--<transformation apply="once" transaction="true">
--<update>
ALTER TABLE users ADD COLUMN last_login TIMESTAMP;
--</update>

--<rollback>
ALTER TABLE users DROP COLUMN last_login;
--</rollback>
--</transformation>
```

#### XML Format

```xml
<transformation apply="once" transaction="true">
    <update>
        ALTER TABLE users ADD COLUMN last_login TIMESTAMP;
    </update>
    <rollback>
        ALTER TABLE users DROP COLUMN last_login;
    </rollback>
</transformation>
```

### Directory Structure

Organize transformations using semantic versioning:

```
transformations/
├── 1.0.0/
│   ├── create_users_table.sql
│   └── create_products_table.sql
├── 1.1.0/
│   └── add_user_preferences.sql
├── 2.0.0/
│   ├── refactor_products.sql
│   └── add_categories.sql
└── 2.1.0/
    └── add_product_images.sql
```

Files are applied in semantic version order, then alphabetically within each version.

## Transformation Formats

### Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `apply` | String | `modified` | When to apply: `once`, `modified`, or `always` |
| `transaction` | Boolean | `true` | Whether to run in a transaction |
| `enabled` | Boolean | `true` | Whether transformation is enabled |

### File Naming Conventions

- Prefix with `-` to skip: `-create_temp_table.sql` (skipped)
- Use semantic versioning in directories: `1.0.0/`, `2.1.3/`
- Use descriptive names: `create_users_table.sql`, `add_email_index.sql`

## Apply Modes

The framework supports three apply modes:

| Mode | Behavior | Use Case | Re-runs on Hash Change? |
|------|----------|----------|------------------------|
| **Once** | Runs only the first time | Initial schema setup, adding columns | No |
| **Modified** | Re-runs when update script changes | View definitions, stored procedures | Yes |
| **Always** | Runs every time | Data cleanup, statistics updates | N/A (always runs) |

### Decision Matrix

```
Has transformation been applied before?
├─ No  → Apply update script
└─ Yes → Check apply mode
    ├─ Once     → Skip (already applied)
    ├─ Modified → Compare hash
    │   ├─ Changed     → Rollback + Apply
    │   └─ Not changed → Skip
    └─ Always   → Rollback + Apply
```

### Examples

```sql
-- Run once: Initial table creation
--<transformation apply="once">
--<update>
CREATE TABLE settings (key VARCHAR(100), value TEXT);
--</update>
--<rollback>
DROP TABLE settings;
--</rollback>
--</transformation>

-- Run when modified: View definition
--<transformation apply="modified">
--<update>
CREATE OR REPLACE VIEW active_users AS 
SELECT * FROM users WHERE deleted_at IS NULL;
--</update>
--<rollback>
DROP VIEW IF EXISTS active_users;
--</rollback>
--</transformation>

-- Run always: Cleanup script
--<transformation apply="always">
--<update>
DELETE FROM sessions WHERE expires_at < NOW();
--</update>
--<rollback>
-- No rollback needed for cleanup
--</rollback>
--</transformation>
```

## Configuration

### Custom Transformation Path

```scala
trait MyTransformations extends FileLocalTransformations {
  override val transformationsPath = "/path/to/transformations"
}
```

### Custom Table Name

```scala
trait MyStoredTransformations extends SlickStoredTransformations {
  override val transformationsTableName = "my_migrations"
}
```

### Database Configuration

```scala
import scala.slick.jdbc.JdbcBackend.Database
import scala.slick.driver.H2Driver

val db = Database.forConfig("mydb") // from application.conf

// Or programmatically:
val db = Database.forURL(
  url = "jdbc:postgresql://localhost/mydb",
  user = "user",
  password = "pass",
  driver = "org.postgresql.Driver"
)
```

## Architecture

The framework uses a trait-based architecture for flexibility:

```
┌─────────────────────────────────────────────────┐
│           Your Application                      │
│  (Combines traits to create custom solution)   │
└─────────────────────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┐
        │             │             │
        ▼             ▼             ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐
│Transformations│ │LocalTrans-   │ │StoredTrans-          │
│              │ │formations    │ │formations            │
│              │ │              │ │                      │
│ Core logic   │ │ File loading │ │ Database persistence │
│ Apply/       │ │ Parsing      │ │ CRUD operations      │
│ Rollback     │ │ Ordering     │ │ Script execution     │
└──────────────┘ └──────────────┘ └──────────────────────┘
                      │                     │
                      ▼                     ▼
              ┌──────────────┐      ┌──────────────┐
              │File          │      │Slick         │
              │LocalTrans-   │      │StoredTrans-  │
              │formations    │      │formations    │
              │              │      │              │
              │Semantic ver  │      │SQL table     │
              │SQL/XML parse │      │Hash tracking │
              └──────────────┘      └──────────────┘
```

### Core Components

1. **Transformation** - Base trait for all transformations
2. **LocalTransformations** - Loads transformations from source (files, resources)
3. **StoredTransformations** - Persists applied transformations to database
4. **Transformations** - Main logic for applying and rolling back changes

See [DOCUMENTATION.md](DOCUMENTATION.md) for detailed architecture information.

## Advanced Features

### Skip Transformations

Prefix filename with `-` to skip:

```
transformations/
├── 1.0.0/
│   ├── create_users.sql          # Applied
│   └── -create_temp_table.sql    # Skipped
```

### Disable Transformations

Set `enabled="false"` to rollback and disable:

```xml
<transformation enabled="false">
    <update>CREATE TABLE deprecated_feature ...</update>
    <rollback>DROP TABLE deprecated_feature;</rollback>
</transformation>
```

When a transformation is disabled:
- If previously applied → Runs rollback script and removes from tracking
- If not applied → Skipped entirely

### Custom Lifecycle Hooks

Override methods to add custom behavior:

```scala
class MonitoredTransformations extends Transformations 
  with FileLocalTransformations 
  with SlickStoredTransformations {
  
  override protected def onApply(local: LocalTransformation): Unit = {
    // Pre-apply hook
    logger.info(s"Starting ${local.id}")
    val start = System.currentTimeMillis()
    
    // Run transformation
    super.onApply(local)
    
    // Post-apply hook
    val duration = System.currentTimeMillis() - start
    metrics.recordMigration(local.id, duration)
  }
  
  override protected def onRollback(transformation: Transformation): Unit = {
    // Custom rollback logging
    notifications.send(s"Rolling back ${transformation.id}")
    super.onRollback(transformation)
  }
}
```

## Testing

### Running Tests

```bash
./gradlew test
```

### Writing Tests

The framework provides mock implementations for testing:

```scala
import org.scalatest.FlatSpec
import org.mercuree.transformations.core._

class MyMigrationSpec extends FlatSpec {
  "User table migration" should "create users table" in {
    val transformation = LocalTransformation.fromFile(
      new File("transformations/1.0.0/create_users.sql"),
      "1.0.0/create_users.sql"
    )
    
    assert(transformation.isInstanceOf[LocalTransformation])
    assert(transformation.id == "1.0.0/create_users.sql")
  }
}
```

See [EXAMPLES.md](EXAMPLES.md) for more testing examples.

## Requirements

- **Scala**: 2.10.x (for Scala 2.13+ see [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md))
- **Java**: 6 or higher
- **Slick**: 2.0.x
- **Database**: Any JDBC-compatible database (H2, PostgreSQL, MySQL, etc.)
- **Build Tool**: Gradle 1.10+ or SBT

## Contributing

We welcome contributions! Here's how you can help:

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/my-feature`
3. **Write tests** for your changes
4. **Ensure tests pass**: `./gradlew test`
5. **Commit your changes**: `git commit -am 'Add my feature'`
6. **Push to the branch**: `git push origin feature/my-feature`
7. **Submit a pull request**

### Code Style

- Follow Scala best practices
- Add ScalaDoc comments for public APIs
- Include unit tests for new features
- Keep changes focused and atomic

## Roadmap

### Current Version: 1.0.0

### Planned Features

- [ ] **Scala 3 Support** - Migrate to Scala 3.x
- [ ] **Slick 3.x Support** - Upgrade to latest Slick version
- [ ] **Dependency Management** - Support for transformation dependencies
- [ ] **Dry Run Mode** - Preview changes without applying
- [ ] **Migration Reports** - Generate HTML/PDF migration reports
- [ ] **Checksum Validation** - Detect manual database changes
- [ ] **Parallel Execution** - Run independent transformations in parallel
- [ ] **Cloud Storage** - Load transformations from S3, Azure Blob
- [ ] **Multi-tenancy** - Support for tenant-specific migrations
- [ ] **Audit Logging** - Detailed audit trail of all changes

### Under Consideration

- Integration with Flyway/Liquibase
- GraphQL schema migrations
- NoSQL database support
- Migration generation from ORM models

## Documentation

- **[QUICKSTART.md](QUICKSTART.md)** - 5-minute getting started guide
- **[DOCUMENTATION.md](DOCUMENTATION.md)** - Comprehensive technical documentation
- **[EXAMPLES.md](EXAMPLES.md)** - Real-world examples and patterns
- **[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)** - Guide for upgrading Scala and dependencies

## License

Copyright 2014 the original author or authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

---

Made with ❤️ by the Transformations Framework community
