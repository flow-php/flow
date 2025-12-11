# Role and Grant Query Builder

The pg-query library provides fluent builders for managing PostgreSQL roles, users, and their privileges.

## Role Management

### CREATE ROLE

Create database roles with various options:

```php
use function Flow\PgQuery\DSL\create;

// Simple role
create()->role('admin')->toAst();
// CREATE ROLE admin

// Role with login (equivalent to CREATE USER)
create()->role('app_user')
    ->login()
    ->withPassword('secret')
    ->toAst();
// CREATE ROLE app_user LOGIN PASSWORD 'secret'

// Superuser with multiple options
create()->role('admin')
    ->superuser()
    ->login()
    ->createDb()
    ->createRole()
    ->connectionLimit(10)
    ->validUntil('2025-12-31')
    ->toAst();
// CREATE ROLE admin SUPERUSER LOGIN CREATEDB CREATEROLE CONNECTION LIMIT 10 VALID UNTIL '2025-12-31'

// Role that inherits from another role
create()->role('developer')
    ->login()
    ->inRole('team_lead')
    ->toAst();
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
use function Flow\PgQuery\DSL\alter;

// Change options
alter()->role('admin')
    ->superuser()
    ->toAst();
// ALTER ROLE admin SUPERUSER

alter()->role('user')
    ->noLogin()
    ->connectionLimit(5)
    ->toAst();
// ALTER ROLE user NOLOGIN CONNECTION LIMIT 5

// Rename role
alter()->role('old_name')
    ->renameTo('new_name')
    ->toAst();
// ALTER ROLE old_name RENAME TO new_name
```

### DROP ROLE

Remove roles from the database:

```php
use function Flow\PgQuery\DSL\drop;

// Simple drop
drop()->role('admin')->toAst();
// DROP ROLE admin

// Drop if exists
drop()->role('admin')
    ->ifExists()
    ->toAst();
// DROP ROLE IF EXISTS admin

// Drop multiple roles
drop()->role('role1', 'role2', 'role3')
    ->ifExists()
    ->toAst();
// DROP ROLE IF EXISTS role1, role2, role3
```

## Privilege Management

### GRANT Privileges

Grant object privileges to roles:

```php
use function Flow\PgQuery\DSL\grant;
use Flow\PgQuery\QueryBuilder\Schema\Grant\TablePrivilege;

// Grant SELECT on a table
grant(TablePrivilege::SELECT)
    ->onTable('users')
    ->to('app_user')
    ->toAst();
// GRANT SELECT ON users TO app_user

// Grant multiple privileges
grant(TablePrivilege::SELECT, TablePrivilege::INSERT, TablePrivilege::UPDATE)
    ->onTable('orders')
    ->to('order_processor')
    ->toAst();
// GRANT SELECT, INSERT, UPDATE ON orders TO order_processor

// Grant ALL privileges
grant(TablePrivilege::ALL)
    ->onTable('products')
    ->to('admin')
    ->toAst();
// GRANT ALL ON products TO admin

// Grant on all tables in schema
grant(TablePrivilege::SELECT)
    ->onAllTablesInSchema('public')
    ->to('reader')
    ->toAst();
// GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader

// Grant to PUBLIC
grant(TablePrivilege::SELECT)
    ->onTable('public_data')
    ->toPublic()
    ->toAst();
// GRANT SELECT ON public_data TO PUBLIC

// Grant with GRANT OPTION
grant(TablePrivilege::SELECT)
    ->onTable('shared_data')
    ->to('team_lead')
    ->withGrantOption()
    ->toAst();
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
use function Flow\PgQuery\DSL\grant_role;

// Grant role to user
grant_role('admin')
    ->to('user1')
    ->toAst();
// GRANT admin TO user1

// Grant multiple roles
grant_role('admin', 'developer')
    ->to('team_lead')
    ->toAst();
// GRANT admin, developer TO team_lead

// Grant with admin option
grant_role('admin')
    ->to('super_admin')
    ->withAdminOption()
    ->toAst();
// GRANT admin TO super_admin WITH ADMIN OPTION
```

### REVOKE Privileges

Revoke object privileges from roles:

```php
use function Flow\PgQuery\DSL\revoke;
use Flow\PgQuery\QueryBuilder\Schema\Grant\TablePrivilege;

// Revoke SELECT
revoke(TablePrivilege::SELECT)
    ->onTable('users')
    ->from('app_user')
    ->toAst();
// REVOKE SELECT ON users FROM app_user

// Revoke with CASCADE
revoke(TablePrivilege::ALL)
    ->onTable('sensitive_data')
    ->from('former_employee')
    ->cascade()
    ->toAst();
// REVOKE ALL ON sensitive_data FROM former_employee CASCADE

// Revoke from PUBLIC
revoke(TablePrivilege::SELECT)
    ->onTable('public_data')
    ->fromPublic()
    ->toAst();
// REVOKE SELECT ON public_data FROM PUBLIC
```

### REVOKE Role Membership

Revoke role membership:

```php
use function Flow\PgQuery\DSL\revoke_role;

// Simple revoke
revoke_role('admin')
    ->from('user1')
    ->toAst();
// REVOKE admin FROM user1

// Revoke with CASCADE
revoke_role('admin')
    ->from('user1')
    ->cascade()
    ->toAst();
// REVOKE admin FROM user1 CASCADE
```

## Session Management

### SET ROLE

Change the current session role:

```php
use function Flow\PgQuery\DSL\set_role;

set_role('admin')->toAst();
// SET ROLE admin
```

### RESET ROLE

Reset to the original role:

```php
use function Flow\PgQuery\DSL\reset_role;

reset_role()->toAst();
// RESET ROLE
```

## Ownership Management

### REASSIGN OWNED

Reassign ownership of database objects:

```php
use function Flow\PgQuery\DSL\reassign_owned;

reassign_owned('old_role')
    ->to('new_role')
    ->toAst();
// REASSIGN OWNED BY old_role TO new_role

// Multiple source roles
reassign_owned('role1', 'role2')
    ->to('new_owner')
    ->toAst();
// REASSIGN OWNED BY role1, role2 TO new_owner
```

### DROP OWNED

Drop objects owned by roles:

```php
use function Flow\PgQuery\DSL\drop_owned;

drop_owned('old_role')->toAst();
// DROP OWNED BY old_role

drop_owned('role1', 'role2')
    ->cascade()
    ->toAst();
// DROP OWNED BY role1, role2 CASCADE
```
