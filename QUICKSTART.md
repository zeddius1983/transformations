# Quick Start Guide

Get started with the Transformations Framework in 5 minutes! This guide walks you through creating your first database migration from scratch.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Step-by-Step Tutorial](#step-by-step-tutorial)
- [Complete Example](#complete-example)
- [Common Use Cases](#common-use-cases)
- [Troubleshooting](#troubleshooting)
- [Next Steps](#next-steps)

## Prerequisites

Before you begin, ensure you have:

- ✅ Java 6 or higher installed
- ✅ Scala 2.10+ installed
- ✅ Gradle 1.10+ or SBT build tool
- ✅ A JDBC-compatible database (we'll use H2 in-memory for this tutorial)

## Step-by-Step Tutorial

### Step 1: Set Up Your Project

Create a new Scala project or use an existing one:

```bash
mkdir my-migration-project
cd my-migration-project
```

Create a `build.gradle` file:

```gradle
apply plugin: 'scala'

repositories {
    mavenCentral()
}

dependencies {
    compile 'org.scala-lang:scala-library:2.10.3'
    compile 'org.mercuree:transformations-core:1.0.0'
    compile 'com.typesafe.slick:slick_2.10:2.0.0'
    compile 'com.h2database:h2:1.3.175'
    compile 'org.slf4j:slf4j-api:1.7.5'
    runtime 'ch.qos.logback:logback-classic:1.0.13'
}

sourceCompatibility = 1.6
targetCompatibility = 1.6
```

### Step 2: Create Transformation Directory

Create a directory structure for your transformations:

```bash
mkdir -p src/main/resources/transformations/1.0.0
```

### Step 3: Write Your First Transformation

Create `src/main/resources/transformations/1.0.0/01_create_users_table.sql`:

```sql
--<transformation apply="once" transaction="true">
--<update>
CREATE TABLE users (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_created_at ON users(created_at);
--</update>

--<rollback>
DROP INDEX IF EXISTS idx_users_created_at;
DROP INDEX IF EXISTS idx_users_email;
DROP TABLE IF EXISTS users;
--</rollback>
--</transformation>
```

### Step 4: Create the Migration Runner

Create `src/main/scala/com/example/MigrationRunner.scala`:

```scala
package com.example

import org.mercuree.transformations.core._
import scala.slick.jdbc.JdbcBackend.Database
import scala.slick.driver.H2Driver

object MigrationRunner extends App {
  
  // Create database connection
  val db = Database.forURL(
    url = "jdbc:h2:./myapp;AUTO_SERVER=TRUE",
    driver = "org.h2.Driver"
  )
  
  // Create migration instance
  object Migrations extends Transformations 
    with FileLocalTransformations 
    with SlickStoredTransformations {
    
    // Configure path to transformations
    override val transformationsPath = "/transformations"
    
    // Configure database driver
    override val profile = H2Driver
    
    // Pass database connection
    override val db = MigrationRunner.db
  }
  
  // Run migrations
  println("Starting database migration...")
  try {
    Migrations.run
    println("✓ Migration completed successfully!")
  } catch {
    case e: Exception =>
      println(s"✗ Migration failed: ${e.getMessage}")
      e.printStackTrace()
  } finally {
    db.close()
  }
}
```

### Step 5: Run Your Migration

Execute the migration:

```bash
./gradlew run
```

You should see output like:

```
Starting database migration...
[INFO] Applying [1.0.0/01_create_users_table.sql]
[INFO] > [1.0.0/01_create_users_table.sql] is ran for the first time
[INFO] [1.0.0/01_create_users_table.sql] processed in 45 ms
✓ Migration completed successfully!
```

🎉 **Congratulations!** You've just run your first database migration!

## Complete Example

Here's a complete example with multiple transformations:

### Directory Structure

```
my-migration-project/
├── build.gradle
└── src/
    └── main/
        ├── resources/
        │   └── transformations/
        │       ├── 1.0.0/
        │       │   ├── 01_create_users_table.sql
        │       │   └── 02_create_products_table.sql
        │       └── 1.1.0/
        │           └── 01_add_user_preferences.sql
        └── scala/
            └── com/
                └── example/
                    ├── MigrationRunner.scala
                    └── Main.scala
```

### Example: Create Products Table

`transformations/1.0.0/02_create_products_table.sql`:

```sql
--<transformation apply="once">
--<update>
CREATE TABLE products (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by BIGINT,
    FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE INDEX idx_products_name ON products(name);
CREATE INDEX idx_products_price ON products(price);
--</update>

--<rollback>
DROP INDEX IF EXISTS idx_products_price;
DROP INDEX IF EXISTS idx_products_name;
DROP TABLE IF EXISTS products;
--</rollback>
--</transformation>
```

### Example: Add User Preferences

`transformations/1.1.0/01_add_user_preferences.sql`:

```sql
--<transformation apply="once">
--<update>
-- Add preferences columns to users table
ALTER TABLE users ADD COLUMN notification_enabled BOOLEAN DEFAULT TRUE;
ALTER TABLE users ADD COLUMN theme VARCHAR(20) DEFAULT 'light';
ALTER TABLE users ADD COLUMN language VARCHAR(10) DEFAULT 'en';

-- Create index for frequently queried fields
CREATE INDEX idx_users_language ON users(language);
--</update>

--<rollback>
-- Remove indexes
DROP INDEX IF EXISTS idx_users_language;

-- Remove columns
ALTER TABLE users DROP COLUMN language;
ALTER TABLE users DROP COLUMN theme;
ALTER TABLE users DROP COLUMN notification_enabled;
--</rollback>
--</transformation>
```

## Common Use Cases

### Use Case 1: Initial Schema Setup

**Scenario**: Setting up your database for the first time.

```sql
--<transformation apply="once">
--<update>
-- Create all initial tables
CREATE TABLE users (...);
CREATE TABLE products (...);
CREATE TABLE orders (...);
CREATE TABLE order_items (...);

-- Add foreign keys
ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id);
ALTER TABLE order_items ADD FOREIGN KEY (order_id) REFERENCES orders(id);
ALTER TABLE order_items ADD FOREIGN KEY (product_id) REFERENCES products(id);
--</update>

--<rollback>
-- Drop in reverse order (due to foreign keys)
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS users;
--</rollback>
--</transformation>
```

### Use Case 2: Adding a New Table

**Scenario**: Adding a new feature that requires a new table.

```sql
--<transformation apply="once">
--<update>
CREATE TABLE user_sessions (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    token VARCHAR(255) NOT NULL UNIQUE,
    ip_address VARCHAR(45),
    user_agent TEXT,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX idx_sessions_token ON user_sessions(token);
CREATE INDEX idx_sessions_user_id ON user_sessions(user_id);
CREATE INDEX idx_sessions_expires_at ON user_sessions(expires_at);
--</update>

--<rollback>
DROP INDEX IF EXISTS idx_sessions_expires_at;
DROP INDEX IF EXISTS idx_sessions_user_id;
DROP INDEX IF EXISTS idx_sessions_token;
DROP TABLE IF EXISTS user_sessions;
--</rollback>
--</transformation>
```

### Use Case 3: Modifying Existing Tables

**Scenario**: Adding columns to an existing table.

```sql
--<transformation apply="once">
--<update>
-- Add new columns
ALTER TABLE users ADD COLUMN phone VARCHAR(20);
ALTER TABLE users ADD COLUMN verified BOOLEAN DEFAULT FALSE;
ALTER TABLE users ADD COLUMN verification_token VARCHAR(255);

-- Add index for verification lookups
CREATE INDEX idx_users_verification_token ON users(verification_token);
--</update>

--<rollback>
DROP INDEX IF EXISTS idx_users_verification_token;
ALTER TABLE users DROP COLUMN verification_token;
ALTER TABLE users DROP COLUMN verified;
ALTER TABLE users DROP COLUMN phone;
--</rollback>
--</transformation>
```

### Use Case 4: Data Migration

**Scenario**: Migrating existing data to a new structure.

```sql
--<transformation apply="once">
--<update>
-- Add new normalized phone table
CREATE TABLE user_phones (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    phone_number VARCHAR(20) NOT NULL,
    phone_type VARCHAR(20) DEFAULT 'mobile',
    is_primary BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Migrate existing phone data
INSERT INTO user_phones (user_id, phone_number, is_primary)
SELECT id, phone, TRUE
FROM users
WHERE phone IS NOT NULL AND phone != '';

-- Remove old column
ALTER TABLE users DROP COLUMN phone;
--</update>

--<rollback>
-- Add column back
ALTER TABLE users ADD COLUMN phone VARCHAR(20);

-- Restore data
UPDATE users u
SET phone = (
    SELECT phone_number
    FROM user_phones p
    WHERE p.user_id = u.id AND p.is_primary = TRUE
    LIMIT 1
);

-- Drop new table
DROP TABLE IF EXISTS user_phones;
--</rollback>
--</transformation>
```

## Troubleshooting

### Problem: Migration doesn't run

**Symptom**: No output or transformations aren't being applied.

**Solution**:
1. Check that `transformationsPath` matches your directory structure
2. Verify files end with `.sql` or `.xml`
3. Ensure transformation XML tags are properly formatted
4. Check logs for parsing errors

```scala
// Enable debug logging
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{Level, Logger}

val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
  .asInstanceOf[Logger]
root.setLevel(Level.DEBUG)
```

### Problem: "Table already exists" error

**Symptom**: Error when running migration that creates a table.

**Solution**: The transformation may have been partially applied. Options:

1. **Clean slate** (for development):
   ```sql
   -- Manually drop tables and transformations table
   DROP TABLE IF EXISTS transformations;
   ```

2. **Fix the transformation**:
   Use `IF NOT EXISTS` clauses:
   ```sql
   CREATE TABLE IF NOT EXISTS users (...);
   ```

### Problem: Foreign key constraint errors

**Symptom**: Rollback fails due to foreign key constraints.

**Solution**: Drop tables in reverse order in rollback:

```sql
--<rollback>
-- Drop child tables first
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;

-- Then parent tables
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS users;
--</rollback>
```

### Problem: Transformation runs every time

**Symptom**: Same transformation executes on every run.

**Solution**: Check the `apply` attribute:

```sql
-- Change from "always"
--<transformation apply="always">

-- To "once"
--<transformation apply="once">
```

### Problem: Can't connect to database

**Symptom**: Database connection errors.

**Solution**: Verify your connection string:

```scala
// H2 File Database
Database.forURL("jdbc:h2:./mydb", driver = "org.h2.Driver")

// PostgreSQL
Database.forURL(
  "jdbc:postgresql://localhost:5432/mydb",
  user = "postgres",
  password = "password",
  driver = "org.postgresql.Driver"
)

// MySQL
Database.forURL(
  "jdbc:mysql://localhost:3306/mydb",
  user = "root",
  password = "password",
  driver = "com.mysql.jdbc.Driver"
)
```

## Next Steps

Now that you've completed the quick start, explore more advanced topics:

### Learn More

1. **[DOCUMENTATION.md](DOCUMENTATION.md)** - Deep dive into architecture and advanced features
2. **[EXAMPLES.md](EXAMPLES.md)** - More real-world examples and patterns
3. **[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)** - Upgrade to Scala 2.13 or 3.x
4. **[README.md](README.md)** - Complete overview and feature list

### Explore Advanced Features

- **Apply Modes**: Learn when to use `once`, `modified`, and `always`
- **Custom Hooks**: Add monitoring and notifications to your migrations
- **Multiple Databases**: Support different database types in one project
- **Transaction Control**: Fine-tune transaction boundaries
- **Testing**: Write comprehensive tests for your migrations

### Best Practices

1. **Always write rollback scripts** - Even for "simple" changes
2. **Test on a copy of production data** - Before applying to production
3. **Use semantic versioning** - Keep transformations organized
4. **Make incremental changes** - Smaller transformations are safer
5. **Review generated SQL** - Know exactly what will be executed
6. **Back up before migrating** - Safety first!

### Join the Community

- Report issues on [GitHub](https://github.com/zeddius1983/transformations/issues)
- Contribute improvements via Pull Requests
- Share your use cases and patterns

---

**Happy Migrating! 🚀**
