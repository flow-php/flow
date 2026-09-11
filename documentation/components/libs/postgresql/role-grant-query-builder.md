# Role and Grant Query Builder

[DOC_LINK:/documentation/components/libs/postgresql]

The PostgreSQL library provides fluent builders for managing PostgreSQL roles, users, and their privileges.

## Role Management

### CREATE ROLE

Create database roles with various options:

```php
use function Flow\PostgreSql\DSL\create;

// Simple role
create()->role('admin')->toSql();
// CREATE ROLE admin

// Role with login (equivalent to CREATE USER)
create()->role('app_user')
    ->login()
    ->withPassword('secret')
    ->toSql();
// CREATE ROLE app_user LOGIN PASSWORD 'secret'

// Superuser with multiple options
create()->role('admin')
    ->superuser()
    ->login()
    ->createDb()
    ->createRole()
    ->connectionLimit(10)
    ->validUntil('2025-12-31')
    ->toSql();
// CREATE ROLE admin SUPERUSER LOGIN CREATEDB CREATEROLE CONNECTION LIMIT 10 VALID UNTIL '2025-12-31'

// Role that inherits from another role
create()->role('developer')
    ->login()
    ->inRole('team_lead')
    ->toSql();
// CREATE ROLE developer LOGIN IN ROLE team_lead
```

Available options:
- `superuser()` / `noSuperuser()` - Superuser privileges
- `createDb()` / `noCreateDb()` - Database creation privileges
- `createRole()` / `noCreateRole()` - Role creation privileges
- `login()` / `noLogin()` - Connection privileges (login makes it a "user")
- `inherit()` / `noInherit()` - Privilege inheritance
- `replication()` / `noReplication()` - Replication privileges
- `bypassRls()` / `noBypassRls()` - Row-level security bypass
- `withPassword(string)` - Set password
- `connectionLimit(int)` - Maximum concurrent connections
- `validUntil(string)` - Password expiration date
- `inRole(string)` - Member of another role

### ALTER ROLE

Modify existing roles:

```php
use function Flow\PostgreSql\DSL\alter;

// Change options
alter()->role('admin')
    ->set()
    ->superuser()
    ->toSql();
// ALTER ROLE admin WITH SUPERUSER

alter()->role('user')
    ->set()
    ->noLogin()
    ->connectionLimit(5)
    ->toSql();
// ALTER ROLE user WITH NOLOGIN CONNECTION LIMIT 5

// Rename role
alter()->role('old_name')
    ->renameTo('new_name')
    ->toSql();
// ALTER ROLE old_name RENAME TO new_name
```

### DROP ROLE

Remove roles from the database:

```php
use function Flow\PostgreSql\DSL\drop;

// Simple drop
drop()->role('admin')->toSql();
// DROP ROLE admin

// Drop if exists
drop()->role('admin')
    ->ifExists()
    ->toSql();
// DROP ROLE IF EXISTS admin

// Drop multiple roles
drop()->role('role1', 'role2', 'role3')
    ->ifExists()
    ->toSql();
// DROP ROLE IF EXISTS role1, role2, role3
```

## Privilege Management

### GRANT Privileges

Grant object privileges to roles:

```php
use function Flow\PostgreSql\DSL\grant;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\TablePrivilege;

// Grant SELECT on a table
grant(TablePrivilege::SELECT)
    ->onTable('users')
    ->to('app_user')
    ->toSql();
// GRANT SELECT ON users TO app_user

// Grant multiple privileges
grant(TablePrivilege::SELECT, TablePrivilege::INSERT, TablePrivilege::UPDATE)
    ->onTable('orders')
    ->to('order_processor')
    ->toSql();
// GRANT SELECT, INSERT, UPDATE ON orders TO order_processor

// Grant ALL privileges
grant(TablePrivilege::ALL)
    ->onTable('products')
    ->to('admin')
    ->toSql();
// GRANT ALL ON products TO admin

// Grant on all tables in schema
grant(TablePrivilege::SELECT)
    ->onAllTablesInSchema('public')
    ->to('reader')
    ->toSql();
// GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader

// Grant to PUBLIC
grant(TablePrivilege::SELECT)
    ->onTable('public_data')
    ->toPublic()
    ->toSql();
// GRANT SELECT ON public_data TO PUBLIC

// Grant with GRANT OPTION
grant(TablePrivilege::SELECT)
    ->onTable('shared_data')
    ->to('team_lead')
    ->withGrantOption()
    ->toSql();
// GRANT SELECT ON shared_data TO team_lead WITH GRANT OPTION
```

Available privileges:
- `TablePrivilege::SELECT`
- `TablePrivilege::INSERT`
- `TablePrivilege::UPDATE`
- `TablePrivilege::DELETE`
- `TablePrivilege::TRUNCATE`
- `TablePrivilege::REFERENCES`
- `TablePrivilege::TRIGGER`
- `TablePrivilege::ALL`

### GRANT Role Membership

Grant role membership to other roles:

```php
use function Flow\PostgreSql\DSL\grant_role;

// Grant role to user
grant_role('admin')
    ->to('user1')
    ->toSql();
// GRANT admin TO user1

// Grant multiple roles
grant_role('admin', 'developer')
    ->to('team_lead')
    ->toSql();
// GRANT admin, developer TO team_lead

// Grant with admin option
grant_role('admin')
    ->to('super_admin')
    ->withAdminOption()
    ->toSql();
// GRANT admin TO super_admin WITH ADMIN OPTION
```

### REVOKE Privileges

Revoke object privileges from roles:

```php
use function Flow\PostgreSql\DSL\revoke;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\TablePrivilege;

// Revoke SELECT
revoke(TablePrivilege::SELECT)
    ->onTable('users')
    ->from('app_user')
    ->toSql();
// REVOKE SELECT ON users FROM app_user

// Revoke with CASCADE
revoke(TablePrivilege::ALL)
    ->onTable('sensitive_data')
    ->from('former_employee')
    ->cascade()
    ->toSql();
// REVOKE ALL ON sensitive_data FROM former_employee CASCADE

// Revoke from PUBLIC
revoke(TablePrivilege::SELECT)
    ->onTable('public_data')
    ->fromPublic()
    ->toSql();
// REVOKE SELECT ON public_data FROM PUBLIC
```

### REVOKE Role Membership

Revoke role membership:

```php
use function Flow\PostgreSql\DSL\revoke_role;

// Simple revoke
revoke_role('admin')
    ->from('user1')
    ->toSql();
// REVOKE admin FROM user1

// Revoke with CASCADE
revoke_role('admin')
    ->from('user1')
    ->cascade()
    ->toSql();
// REVOKE admin FROM user1 CASCADE
```

## Session Management

### SET ROLE

Change the current session role:

```php
use function Flow\PostgreSql\DSL\set_role;

set_role('admin')->toSql();
// SET ROLE admin
```

### RESET ROLE

Reset to the original role:

```php
use function Flow\PostgreSql\DSL\reset_role;

reset_role()->toSql();
// RESET ROLE
```

## Ownership Management

### REASSIGN OWNED

Reassign ownership of database objects:

```php
use function Flow\PostgreSql\DSL\reassign_owned;

reassign_owned('old_role')
    ->to('new_role')
    ->toSql();
// REASSIGN OWNED BY old_role TO new_role

// Multiple source roles
reassign_owned('role1', 'role2')
    ->to('new_owner')
    ->toSql();
// REASSIGN OWNED BY role1, role2 TO new_owner
```

### DROP OWNED

Drop objects owned by roles:

```php
use function Flow\PostgreSql\DSL\drop_owned;

drop_owned('old_role')->toSql();
// DROP OWNED BY old_role

drop_owned('role1', 'role2')
    ->cascade()
    ->toSql();
// DROP OWNED BY role1, role2 CASCADE
```
