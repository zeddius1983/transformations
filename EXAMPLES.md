# Examples

Comprehensive real-world examples for the Transformations Framework.

## Table of Contents

- [Basic Examples](#basic-examples)
- [Intermediate Examples](#intermediate-examples)
- [Advanced Examples](#advanced-examples)
- [Apply Mode Examples](#apply-mode-examples)
- [Transaction Handling](#transaction-handling)
- [Error Handling](#error-handling)
- [Rollback Scenarios](#rollback-scenarios)
- [Complete Project Example](#complete-project-example)
- [Testing Examples](#testing-examples)

## Basic Examples

### Example 1: Create Table

Simple table creation with proper rollback:

```sql
-- transformations/1.0.0/001_create_users_table.sql
--<transformation apply="once" transaction="true">
--<update>
CREATE TABLE users (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Add indexes for common queries
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

### Example 2: Add Column

Adding a new column to an existing table:

```sql
-- transformations/1.1.0/001_add_user_status.sql
--<transformation apply="once" transaction="true">
--<update>
-- Add status column with default value
ALTER TABLE users 
ADD COLUMN status VARCHAR(20) DEFAULT 'active' NOT NULL;

-- Add index for status filtering
CREATE INDEX idx_users_status ON users(status);

-- Add constraint to ensure valid values
ALTER TABLE users 
ADD CONSTRAINT chk_user_status 
CHECK (status IN ('active', 'inactive', 'suspended', 'deleted'));
--</update>

--<rollback>
-- Remove constraint first
ALTER TABLE users DROP CONSTRAINT IF EXISTS chk_user_status;

-- Remove index
DROP INDEX IF EXISTS idx_users_status;

-- Remove column
ALTER TABLE users DROP COLUMN IF EXISTS status;
--</rollback>
--</transformation>
```

### Example 3: Create Index

Creating indexes for performance:

```sql
-- transformations/1.2.0/001_add_performance_indexes.sql
--<transformation apply="once" transaction="true">
--<update>
-- Composite index for common query pattern
CREATE INDEX idx_users_status_created 
ON users(status, created_at DESC);

-- Index for full-text search (MySQL)
CREATE FULLTEXT INDEX idx_users_fulltext 
ON users(username, email);

-- Unique constraint as index
CREATE UNIQUE INDEX idx_users_email_lower 
ON users(LOWER(email));
--</update>

--<rollback>
DROP INDEX IF EXISTS idx_users_email_lower;
DROP INDEX IF EXISTS idx_users_fulltext;
DROP INDEX IF EXISTS idx_users_status_created;
--</rollback>
--</transformation>
```

### Example 4: Drop Table

Safely dropping a table:

```sql
-- transformations/2.0.0/001_remove_deprecated_table.sql
--<transformation apply="once" transaction="true">
--<update>
-- Archive data before dropping (optional)
CREATE TABLE users_archive AS SELECT * FROM old_users;

-- Drop the old table
DROP TABLE IF EXISTS old_users;
--</update>

--<rollback>
-- Restore from archive
CREATE TABLE old_users AS SELECT * FROM users_archive;

-- Remove archive
DROP TABLE IF EXISTS users_archive;
--</rollback>
--</transformation>
```

## Intermediate Examples

### Example 5: Multiple Related Tables

Creating a complete feature with related tables:

```sql
-- transformations/1.3.0/001_create_blog_feature.sql
--<transformation apply="once" transaction="true">
--<update>
-- Create posts table
CREATE TABLE posts (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    title VARCHAR(255) NOT NULL,
    slug VARCHAR(255) NOT NULL UNIQUE,
    content TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'draft',
    published_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Create comments table
CREATE TABLE comments (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    post_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    content TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Create tags table
CREATE TABLE tags (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL UNIQUE,
    slug VARCHAR(50) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create post-tag association table
CREATE TABLE post_tags (
    post_id BIGINT NOT NULL,
    tag_id BIGINT NOT NULL,
    PRIMARY KEY (post_id, tag_id),
    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);

-- Create indexes
CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_status ON posts(status);
CREATE INDEX idx_posts_published_at ON posts(published_at);
CREATE INDEX idx_posts_slug ON posts(slug);
CREATE INDEX idx_comments_post_id ON comments(post_id);
CREATE INDEX idx_comments_user_id ON comments(user_id);
CREATE INDEX idx_comments_status ON comments(status);
--</update>

--<rollback>
-- Drop in reverse order (child tables first)
DROP INDEX IF EXISTS idx_comments_status;
DROP INDEX IF EXISTS idx_comments_user_id;
DROP INDEX IF EXISTS idx_comments_post_id;
DROP INDEX IF EXISTS idx_posts_slug;
DROP INDEX IF EXISTS idx_posts_published_at;
DROP INDEX IF EXISTS idx_posts_status;
DROP INDEX IF EXISTS idx_posts_user_id;

DROP TABLE IF EXISTS post_tags;
DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS comments;
DROP TABLE IF EXISTS posts;
--</rollback>
--</transformation>
```

### Example 6: Foreign Keys with Cascading

Setting up proper foreign key relationships:

```sql
-- transformations/1.4.0/001_add_user_profiles.sql
--<transformation apply="once" transaction="true">
--<update>
CREATE TABLE user_profiles (
    user_id BIGINT PRIMARY KEY,
    bio TEXT,
    avatar_url VARCHAR(500),
    website VARCHAR(255),
    location VARCHAR(100),
    birth_date DATE,
    
    -- Foreign key with cascade delete
    FOREIGN KEY (user_id) 
        REFERENCES users(id) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE
);

-- One-to-many with restrict
CREATE TABLE user_sessions (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    token VARCHAR(255) NOT NULL UNIQUE,
    ip_address VARCHAR(45),
    user_agent TEXT,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Prevent user deletion if sessions exist
    FOREIGN KEY (user_id) 
        REFERENCES users(id) 
        ON DELETE RESTRICT
);

CREATE INDEX idx_sessions_user_id ON user_sessions(user_id);
CREATE INDEX idx_sessions_token ON user_sessions(token);
CREATE INDEX idx_sessions_expires_at ON user_sessions(expires_at);
--</update>

--<rollback>
DROP INDEX IF EXISTS idx_sessions_expires_at;
DROP INDEX IF EXISTS idx_sessions_token;
DROP INDEX IF EXISTS idx_sessions_user_id;
DROP TABLE IF EXISTS user_sessions;
DROP TABLE IF EXISTS user_profiles;
--</rollback>
--</transformation>
```

### Example 7: Complex Indexes

Creating sophisticated indexes:

```sql
-- transformations/1.5.0/001_optimize_queries.sql
--<transformation apply="once" transaction="true">
--<update>
-- Covering index (includes all columns needed for query)
CREATE INDEX idx_posts_covering 
ON posts(status, published_at, user_id, title);

-- Partial index (PostgreSQL)
CREATE INDEX idx_active_posts 
ON posts(published_at) 
WHERE status = 'published';

-- Expression index (PostgreSQL)
CREATE INDEX idx_posts_title_lower 
ON posts(LOWER(title));

-- Multi-column unique constraint
CREATE UNIQUE INDEX idx_posts_user_slug 
ON posts(user_id, slug);

-- Descending index for sorting
CREATE INDEX idx_posts_created_desc 
ON posts(created_at DESC);
--</update>

--<rollback>
DROP INDEX IF EXISTS idx_posts_created_desc;
DROP INDEX IF EXISTS idx_posts_user_slug;
DROP INDEX IF EXISTS idx_posts_title_lower;
DROP INDEX IF EXISTS idx_active_posts;
DROP INDEX IF EXISTS idx_posts_covering;
--</rollback>
--</transformation>
```

### Example 8: Views

Creating and managing database views:

```sql
-- transformations/1.6.0/001_create_summary_views.sql
--<transformation apply="modified" transaction="true">
--<update>
-- Simple view
CREATE OR REPLACE VIEW active_users AS
SELECT 
    id,
    username,
    email,
    created_at
FROM users
WHERE status = 'active'
  AND deleted_at IS NULL;

-- Complex aggregation view
CREATE OR REPLACE VIEW user_statistics AS
SELECT 
    u.id AS user_id,
    u.username,
    COUNT(DISTINCT p.id) AS post_count,
    COUNT(DISTINCT c.id) AS comment_count,
    MAX(p.published_at) AS last_post_date,
    MAX(c.created_at) AS last_comment_date
FROM users u
LEFT JOIN posts p ON u.id = p.user_id AND p.status = 'published'
LEFT JOIN comments c ON u.id = c.user_id
WHERE u.status = 'active'
GROUP BY u.id, u.username;

-- View with joins
CREATE OR REPLACE VIEW post_details AS
SELECT 
    p.id,
    p.title,
    p.slug,
    p.content,
    p.published_at,
    u.username AS author_name,
    u.email AS author_email,
    COUNT(DISTINCT c.id) AS comment_count,
    COUNT(DISTINCT pt.tag_id) AS tag_count
FROM posts p
INNER JOIN users u ON p.user_id = u.id
LEFT JOIN comments c ON p.id = c.post_id
LEFT JOIN post_tags pt ON p.id = pt.post_id
WHERE p.status = 'published'
GROUP BY p.id, p.title, p.slug, p.content, p.published_at, u.username, u.email;
--</update>

--<rollback>
DROP VIEW IF EXISTS post_details;
DROP VIEW IF EXISTS user_statistics;
DROP VIEW IF EXISTS active_users;
--</rollback>
--</transformation>
```

### Example 9: Stored Procedures

Creating stored procedures (MySQL example):

```sql
-- transformations/1.7.0/001_create_procedures.sql
--<transformation apply="modified" transaction="false">
--<update>
DELIMITER //

-- Procedure to create a new post
CREATE PROCEDURE create_post(
    IN p_user_id BIGINT,
    IN p_title VARCHAR(255),
    IN p_content TEXT,
    OUT p_post_id BIGINT
)
BEGIN
    DECLARE v_slug VARCHAR(255);
    
    -- Generate slug from title
    SET v_slug = LOWER(REPLACE(p_title, ' ', '-'));
    
    -- Insert post
    INSERT INTO posts (user_id, title, slug, content, status)
    VALUES (p_user_id, p_title, v_slug, p_content, 'draft');
    
    SET p_post_id = LAST_INSERT_ID();
END //

-- Procedure to publish a post
CREATE PROCEDURE publish_post(IN p_post_id BIGINT)
BEGIN
    UPDATE posts
    SET status = 'published',
        published_at = NOW()
    WHERE id = p_post_id
      AND status = 'draft';
END //

-- Function to count user posts
CREATE FUNCTION count_user_posts(p_user_id BIGINT)
RETURNS INT
DETERMINISTIC
BEGIN
    DECLARE post_count INT;
    
    SELECT COUNT(*)
    INTO post_count
    FROM posts
    WHERE user_id = p_user_id
      AND status = 'published';
    
    RETURN post_count;
END //

DELIMITER ;
--</update>

--<rollback>
DROP FUNCTION IF EXISTS count_user_posts;
DROP PROCEDURE IF EXISTS publish_post;
DROP PROCEDURE IF EXISTS create_post;
--</rollback>
--</transformation>
```

## Advanced Examples

### Example 10: Data Migration

Migrating data between table structures:

```sql
-- transformations/2.0.0/001_normalize_user_data.sql
--<transformation apply="once" transaction="true">
--<update>
-- Step 1: Create new normalized tables
CREATE TABLE user_emails (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    email VARCHAR(255) NOT NULL,
    is_primary BOOLEAN DEFAULT FALSE,
    verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE KEY unique_email (email)
);

CREATE TABLE user_phones (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    phone VARCHAR(20) NOT NULL,
    phone_type VARCHAR(20) DEFAULT 'mobile',
    is_primary BOOLEAN DEFAULT FALSE,
    verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Step 2: Migrate existing data
INSERT INTO user_emails (user_id, email, is_primary, verified)
SELECT 
    id,
    email,
    TRUE,
    email_verified
FROM users
WHERE email IS NOT NULL AND email != '';

INSERT INTO user_phones (user_id, phone, is_primary, verified)
SELECT 
    id,
    phone,
    TRUE,
    phone_verified
FROM users
WHERE phone IS NOT NULL AND phone != '';

-- Step 3: Drop old columns (after verifying migration)
ALTER TABLE users DROP COLUMN email;
ALTER TABLE users DROP COLUMN phone;
ALTER TABLE users DROP COLUMN email_verified;
ALTER TABLE users DROP COLUMN phone_verified;
--</update>

--<rollback>
-- Step 1: Add columns back
ALTER TABLE users ADD COLUMN email VARCHAR(255);
ALTER TABLE users ADD COLUMN phone VARCHAR(20);
ALTER TABLE users ADD COLUMN email_verified BOOLEAN DEFAULT FALSE;
ALTER TABLE users ADD COLUMN phone_verified BOOLEAN DEFAULT FALSE;

-- Step 2: Restore data from normalized tables
UPDATE users u
INNER JOIN user_emails e ON u.id = e.user_id AND e.is_primary = TRUE
SET u.email = e.email,
    u.email_verified = e.verified;

UPDATE users u
INNER JOIN user_phones p ON u.id = p.user_id AND p.is_primary = TRUE
SET u.phone = p.phone,
    u.phone_verified = p.verified;

-- Step 3: Drop new tables
DROP TABLE IF EXISTS user_phones;
DROP TABLE IF EXISTS user_emails;
--</rollback>
--</transformation>
```

### Example 11: Conditional Logic in SQL

Handling complex data transformations:

```sql
-- transformations/2.1.0/001_standardize_statuses.sql
--<transformation apply="once" transaction="true">
--<update>
-- Standardize status values across different formats
UPDATE posts
SET status = CASE
    WHEN status IN ('published', 'pub', 'active', '1') THEN 'published'
    WHEN status IN ('draft', 'pending', '0') THEN 'draft'
    WHEN status IN ('archived', 'deleted', 'removed') THEN 'archived'
    ELSE 'draft'
END;

-- Add proper constraints after standardization
ALTER TABLE posts
ADD CONSTRAINT chk_post_status
CHECK (status IN ('published', 'draft', 'archived'));

-- Standardize user roles with data migration
ALTER TABLE users ADD COLUMN role VARCHAR(20) DEFAULT 'user';

UPDATE users
SET role = CASE
    WHEN is_admin = TRUE THEN 'admin'
    WHEN is_moderator = TRUE THEN 'moderator'
    WHEN is_premium = TRUE THEN 'premium'
    ELSE 'user'
END;

-- Remove old boolean columns
ALTER TABLE users DROP COLUMN is_admin;
ALTER TABLE users DROP COLUMN is_moderator;
ALTER TABLE users DROP COLUMN is_premium;
--</update>

--<rollback>
-- Restore boolean columns
ALTER TABLE users ADD COLUMN is_admin BOOLEAN DEFAULT FALSE;
ALTER TABLE users ADD COLUMN is_moderator BOOLEAN DEFAULT FALSE;
ALTER TABLE users ADD COLUMN is_premium BOOLEAN DEFAULT FALSE;

-- Restore data
UPDATE users
SET is_admin = (role = 'admin'),
    is_moderator = (role = 'moderator'),
    is_premium = (role = 'premium');

ALTER TABLE users DROP COLUMN role;

-- Remove post constraint
ALTER TABLE posts DROP CONSTRAINT IF EXISTS chk_post_status;
--</rollback>
--</transformation>
```

### Example 12: Multi-Step Transformation

Complex transformation broken into logical steps:

```sql
-- transformations/2.2.0/001_implement_soft_deletes.sql
--<transformation apply="once" transaction="true">
--<update>
-- Step 1: Add soft delete columns to all tables
ALTER TABLE users ADD COLUMN deleted_at TIMESTAMP NULL DEFAULT NULL;
ALTER TABLE posts ADD COLUMN deleted_at TIMESTAMP NULL DEFAULT NULL;
ALTER TABLE comments ADD COLUMN deleted_at TIMESTAMP NULL DEFAULT NULL;

-- Step 2: Create indexes for soft delete queries
CREATE INDEX idx_users_deleted_at ON users(deleted_at);
CREATE INDEX idx_posts_deleted_at ON posts(deleted_at);
CREATE INDEX idx_comments_deleted_at ON comments(deleted_at);

-- Step 3: Update views to exclude soft-deleted records
CREATE OR REPLACE VIEW active_users_v2 AS
SELECT *
FROM users
WHERE deleted_at IS NULL
  AND status = 'active';

CREATE OR REPLACE VIEW active_posts_v2 AS
SELECT *
FROM posts
WHERE deleted_at IS NULL
  AND status = 'published';

-- Step 4: Create stored procedure for soft delete
DELIMITER //
CREATE PROCEDURE soft_delete_user(IN p_user_id BIGINT)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- Soft delete user
    UPDATE users
    SET deleted_at = NOW()
    WHERE id = p_user_id;
    
    -- Soft delete user's posts
    UPDATE posts
    SET deleted_at = NOW()
    WHERE user_id = p_user_id;
    
    -- Soft delete user's comments
    UPDATE comments
    SET deleted_at = NOW()
    WHERE user_id = p_user_id;
    
    COMMIT;
END //
DELIMITER ;
--</update>

--<rollback>
-- Remove stored procedure
DROP PROCEDURE IF EXISTS soft_delete_user;

-- Remove views
DROP VIEW IF EXISTS active_posts_v2;
DROP VIEW IF EXISTS active_users_v2;

-- Remove indexes
DROP INDEX IF EXISTS idx_comments_deleted_at ON comments;
DROP INDEX IF EXISTS idx_posts_deleted_at ON posts;
DROP INDEX IF EXISTS idx_users_deleted_at ON users;

-- Remove columns
ALTER TABLE comments DROP COLUMN deleted_at;
ALTER TABLE posts DROP COLUMN deleted_at;
ALTER TABLE users DROP COLUMN deleted_at;
--</rollback>
--</transformation>
```

### Example 13: Environment-Specific Changes

Applying transformations based on environment:

```sql
-- transformations/dev/001_add_test_data.sql
--<transformation apply="always" transaction="true">
--<update>
-- Clear existing test data
DELETE FROM comments WHERE user_id IN (SELECT id FROM users WHERE email LIKE '%@test.com');
DELETE FROM posts WHERE user_id IN (SELECT id FROM users WHERE email LIKE '%@test.com');
DELETE FROM users WHERE email LIKE '%@test.com';

-- Insert test users
INSERT INTO users (username, email, password_hash, status, role)
VALUES 
    ('testuser1', 'test1@test.com', 'hashed_password', 'active', 'user'),
    ('testuser2', 'test2@test.com', 'hashed_password', 'active', 'user'),
    ('testadmin', 'admin@test.com', 'hashed_password', 'active', 'admin');

-- Insert test posts
INSERT INTO posts (user_id, title, slug, content, status, published_at)
SELECT 
    u.id,
    CONCAT('Test Post ', num),
    CONCAT('test-post-', num),
    'This is test content',
    'published',
    NOW()
FROM users u
CROSS JOIN (SELECT 1 AS num UNION SELECT 2 UNION SELECT 3) numbers
WHERE u.email LIKE '%@test.com'
  AND u.role != 'admin';
--</update>

--<rollback>
-- Clean up test data
DELETE FROM comments WHERE user_id IN (SELECT id FROM users WHERE email LIKE '%@test.com');
DELETE FROM posts WHERE user_id IN (SELECT id FROM users WHERE email LIKE '%@test.com');
DELETE FROM users WHERE email LIKE '%@test.com';
--</rollback>
--</transformation>
```

## Apply Mode Examples

### Example 14: Once Mode - Schema Changes

Use `once` for irreversible schema changes:

```sql
-- transformations/1.0.0/001_initial_schema.sql
--<transformation apply="once" transaction="true">
--<update>
CREATE TABLE users (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
--</update>

--<rollback>
DROP TABLE IF EXISTS users;
--</rollback>
--</transformation>
```

**Behavior**: Runs once when first encountered, never again even if file changes.

### Example 15: Modified Mode - View Definitions

Use `modified` for things that should update when changed:

```sql
-- transformations/1.1.0/001_user_summary_view.sql
--<transformation apply="modified" transaction="true">
--<update>
CREATE OR REPLACE VIEW user_summary AS
SELECT 
    u.id,
    u.username,
    u.email,
    COUNT(DISTINCT p.id) AS post_count,
    COUNT(DISTINCT c.id) AS comment_count,
    MAX(p.created_at) AS last_post_date
FROM users u
LEFT JOIN posts p ON u.id = p.user_id
LEFT JOIN comments c ON u.id = c.user_id
GROUP BY u.id, u.username, u.email;
--</update>

--<rollback>
DROP VIEW IF EXISTS user_summary;
--</rollback>
--</transformation>
```

**Behavior**: 
- First run: Creates view
- File unchanged: Skips
- File modified: Drops and recreates view

### Example 16: Always Mode - Cleanup Scripts

Use `always` for maintenance tasks:

```sql
-- transformations/maintenance/001_cleanup_sessions.sql
--<transformation apply="always" transaction="true">
--<update>
-- Delete expired sessions
DELETE FROM user_sessions
WHERE expires_at < NOW();

-- Delete old soft-deleted records (older than 90 days)
DELETE FROM users
WHERE deleted_at < DATE_SUB(NOW(), INTERVAL 90 DAY);

DELETE FROM posts
WHERE deleted_at < DATE_SUB(NOW(), INTERVAL 90 DAY);

-- Optimize tables
OPTIMIZE TABLE user_sessions;
OPTIMIZE TABLE users;
OPTIMIZE TABLE posts;
--</update>

--<rollback>
-- No rollback for cleanup operations
--</rollback>
--</transformation>
```

**Behavior**: Runs every time migrations are executed.

## Transaction Handling

### Example 17: With Transaction

Most transformations should use transactions:

```sql
--<transformation apply="once" transaction="true">
--<update>
BEGIN;

CREATE TABLE accounts (
    id BIGINT PRIMARY KEY,
    balance DECIMAL(10,2) NOT NULL
);

INSERT INTO accounts (id, balance) VALUES (1, 1000.00);
INSERT INTO accounts (id, balance) VALUES (2, 500.00);

-- If this fails, everything rolls back
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;

COMMIT;
--</update>
--</transformation>
```

### Example 18: Without Transaction

Large operations that might timeout:

```sql
--<transformation apply="once" transaction="false">
--<update>
-- Large data migration that might take a long time
-- Better to do outside transaction to avoid locks

-- Process in batches
INSERT INTO new_users (id, username, email)
SELECT id, username, email
FROM old_users
WHERE id BETWEEN 1 AND 10000;

INSERT INTO new_users (id, username, email)
SELECT id, username, email
FROM old_users
WHERE id BETWEEN 10001 AND 20000;

-- ... more batches ...
--</update>
--</transformation>
```

## Error Handling

### Example 19: Idempotent Transformations

Write transformations that can be run multiple times safely:

```sql
--<transformation apply="once" transaction="true">
--<update>
-- Use IF NOT EXISTS
CREATE TABLE IF NOT EXISTS settings (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT
);

-- Check before adding columns
SET @col_exists = (
    SELECT COUNT(*)
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'users'
      AND COLUMN_NAME = 'verified'
);

SET @sql = IF(@col_exists = 0,
    'ALTER TABLE users ADD COLUMN verified BOOLEAN DEFAULT FALSE',
    'SELECT "Column already exists"');

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Safe index creation
CREATE INDEX IF NOT EXISTS idx_users_verified ON users(verified);
--</update>
--</transformation>
```

### Example 20: Validation Checks

Add validation to catch errors early:

```sql
--<transformation apply="once" transaction="true">
--<update>
-- Validate data before migration
DO $$
BEGIN
    IF (SELECT COUNT(*) FROM users WHERE email IS NULL) > 0 THEN
        RAISE EXCEPTION 'Cannot proceed: Found users with NULL email';
    END IF;
    
    IF (SELECT COUNT(*) FROM users WHERE email = '') > 0 THEN
        RAISE EXCEPTION 'Cannot proceed: Found users with empty email';
    END IF;
END $$;

-- Proceed with migration
ALTER TABLE users MODIFY COLUMN email VARCHAR(255) NOT NULL;
CREATE UNIQUE INDEX idx_users_email ON users(email);
--</update>
--</transformation>
```

## Rollback Scenarios

### Example 21: Simple Rollback

Basic rollback that reverses the update:

```sql
--<transformation>
--<update>
ALTER TABLE users ADD COLUMN age INT;
--</update>

--<rollback>
ALTER TABLE users DROP COLUMN age;
--</rollback>
--</transformation>
```

### Example 22: Data-Preserving Rollback

Rollback that preserves data:

```sql
--<transformation>
--<update>
-- Split name into first_name and last_name
ALTER TABLE users ADD COLUMN first_name VARCHAR(100);
ALTER TABLE users ADD COLUMN last_name VARCHAR(100);

UPDATE users
SET first_name = SUBSTRING_INDEX(name, ' ', 1),
    last_name = SUBSTRING_INDEX(name, ' ', -1);

ALTER TABLE users DROP COLUMN name;
--</update>

--<rollback>
-- Restore original structure
ALTER TABLE users ADD COLUMN name VARCHAR(200);

UPDATE users
SET name = CONCAT(first_name, ' ', last_name);

ALTER TABLE users DROP COLUMN first_name;
ALTER TABLE users DROP COLUMN last_name;
--</rollback>
--</transformation>
```

### Example 23: Complex Rollback with Backup

Rollback using backup tables:

```sql
--<transformation>
--<update>
-- Create backup
CREATE TABLE users_backup AS SELECT * FROM users;

-- Perform risky operation
UPDATE users
SET email = LOWER(email);

DELETE FROM users
WHERE deleted_at IS NOT NULL;

-- Drop backup after success
DROP TABLE users_backup;
--</update>

--<rollback>
-- Restore from backup if it exists
DROP TABLE IF EXISTS users;

CREATE TABLE users AS SELECT * FROM users_backup;

DROP TABLE IF EXISTS users_backup;
--</rollback>
--</transformation>
```

## Complete Project Example

### Project Structure

```
my-app/
├── src/main/
│   ├── scala/
│   │   └── com/example/
│   │       ├── Main.scala
│   │       └── migrations/
│   │           └── MigrationRunner.scala
│   └── resources/
│       └── transformations/
│           ├── 1.0.0/
│           │   ├── 001_create_users.sql
│           │   ├── 002_create_posts.sql
│           │   └── 003_create_comments.sql
│           ├── 1.1.0/
│           │   ├── 001_add_user_profiles.sql
│           │   └── 002_add_post_tags.sql
│           ├── 1.2.0/
│           │   └── 001_add_search_indexes.sql
│           ├── 2.0.0/
│           │   └── 001_normalize_user_data.sql
│           └── views/
│               └── 001_create_views.sql
└── build.gradle
```

### MigrationRunner.scala

```scala
package com.example.migrations

import org.mercuree.transformations.core._
import scala.slick.jdbc.JdbcBackend.Database
import scala.slick.driver.H2Driver
import org.slf4j.LoggerFactory

object MigrationRunner extends App {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Database configuration
  val db = Database.forConfig("database")
  
  // Define migrations
  object AppMigrations extends Transformations 
    with FileLocalTransformations 
    with SlickStoredTransformations {
    
    override val transformationsPath = "/transformations"
    override val profile = H2Driver
    override val db = MigrationRunner.db
    
    // Custom hooks for monitoring
    override protected def onApply(local: LocalTransformation): Unit = {
      logger.info(s"▶ Applying: ${local.id}")
      val start = System.currentTimeMillis()
      
      super.onApply(local)
      
      val duration = System.currentTimeMillis() - start
      logger.info(s"✓ Completed: ${local.id} (${duration}ms)")
    }
    
    override protected def onRollback(transformation: Transformation): Unit = {
      logger.warn(s"◀ Rolling back: ${transformation.id}")
      super.onRollback(transformation)
      logger.warn(s"✓ Rolled back: ${transformation.id}")
    }
  }
  
  // Run migrations
  logger.info("=" * 60)
  logger.info("Starting database migration...")
  logger.info("=" * 60)
  
  try {
    AppMigrations.run()
    logger.info("=" * 60)
    logger.info("✓ Migration completed successfully!")
    logger.info("=" * 60)
  } catch {
    case e: Exception =>
      logger.error("=" * 60)
      logger.error("✗ Migration failed!", e)
      logger.error("=" * 60)
      sys.exit(1)
  } finally {
    db.close()
  }
}
```

### application.conf

```hocon
database {
  url = "jdbc:postgresql://localhost:5432/myapp"
  driver = "org.postgresql.Driver"
  user = "myapp"
  password = "secret"
  
  connectionPool = "HikariCP"
  
  properties = {
    maximumPoolSize = 10
    minimumIdle = 2
  }
}
```

## Testing Examples

### Example 24: Unit Test

```scala
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.mercuree.transformations.core._
import java.io.File

class TransformationParsingSpec extends FlatSpec with Matchers {
  
  "SQL transformation" should "parse correctly" in {
    val sql = """
      |--<transformation apply="once" transaction="true">
      |--<update>
      |CREATE TABLE test (id INT);
      |--</update>
      |--<rollback>
      |DROP TABLE test;
      |--</rollback>
      |--</transformation>
    """.stripMargin
    
    val t = LocalTransformation.parseSQL(sql, "test.sql")
    
    t shouldBe a[LocalTransformation]
    val local = t.asInstanceOf[LocalTransformation]
    local.id shouldBe "test.sql"
    local.updateScript should include("CREATE TABLE")
    local.rollbackScript should include("DROP TABLE")
    local.applyMode shouldBe ApplyMode.Once
    local.runInTransaction shouldBe true
  }
  
  "XML transformation" should "parse correctly" in {
    val xml = scala.xml.XML.loadString("""
      <transformation apply="modified" transaction="false">
        <update>ALTER TABLE users ADD COLUMN age INT;</update>
        <rollback>ALTER TABLE users DROP COLUMN age;</rollback>
      </transformation>
    """)
    
    val t = LocalTransformation.parseXML(xml, "test.xml")
    
    t shouldBe a[LocalTransformation]
    val local = t.asInstanceOf[LocalTransformation]
    local.applyMode shouldBe ApplyMode.Modified
    local.runInTransaction shouldBe false
  }
  
  "Disabled transformation" should "be recognized" in {
    val xml = scala.xml.XML.loadString("""
      <transformation enabled="false">
        <update>CREATE TABLE deprecated ();</update>
        <rollback>DROP TABLE deprecated;</rollback>
      </transformation>
    """)
    
    val t = LocalTransformation.parseXML(xml, "disabled.xml")
    
    t shouldBe a[DisabledTransformation]
  }
  
  "Skipped transformation" should "be recognized" in {
    val t = LocalTransformation.parseSQL(
      "--<transformation><update>CREATE TABLE temp ();</update></transformation>",
      "-skipped.sql"
    )
    
    t shouldBe a[SkippedTransformation]
  }
}
```

### Example 25: Integration Test

```scala
import org.scalatest.{FlatSpec, BeforeAndAfterAll}
import org.mercuree.transformations.core._
import scala.slick.jdbc.JdbcBackend.Database
import scala.slick.driver.H2Driver
import scala.slick.jdbc.{StaticQuery => Sql}

class MigrationIntegrationSpec extends FlatSpec with BeforeAndAfterAll {
  
  var db: Database = _
  
  override def beforeAll(): Unit = {
    db = Database.forURL(
      "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1",
      driver = "org.h2.Driver"
    )
  }
  
  override def afterAll(): Unit = {
    db.close()
  }
  
  "Complete migration" should "create all tables" in {
    object TestMigrations extends Transformations
      with FileLocalTransformations
      with SlickStoredTransformations {
      
      override val transformationsPath = "/test/transformations"
      override val profile = H2Driver
      override val db = MigrationIntegrationSpec.this.db
    }
    
    // Run migrations
    TestMigrations.run()
    
    // Verify tables exist
    db.withSession { implicit session =>
      val tables = Sql.queryNA[String]("SHOW TABLES").list
      tables should contain("USERS")
      tables should contain("POSTS")
      tables should contain("TRANSFORMATIONS")
    }
  }
  
  "Re-running migrations" should "be idempotent" in {
    object TestMigrations extends Transformations
      with FileLocalTransformations
      with SlickStoredTransformations {
      
      override val transformationsPath = "/test/transformations"
      override val profile = H2Driver
      override val db = MigrationIntegrationSpec.this.db
    }
    
    // Run twice
    TestMigrations.run()
    TestMigrations.run()
    
    // Should not fail
    db.withSession { implicit session =>
      val count = Sql.queryNA[Int](
        "SELECT COUNT(*) FROM transformations"
      ).first
      
      count should be > 0
    }
  }
}
```

---

For more examples and patterns:
- [README.md](README.md) - Overview and quick start
- [QUICKSTART.md](QUICKSTART.md) - Step-by-step tutorial
- [DOCUMENTATION.md](DOCUMENTATION.md) - Technical details
- [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) - Upgrading guide
