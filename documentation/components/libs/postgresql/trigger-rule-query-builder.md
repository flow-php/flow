# Trigger and Rule Query Builder

This document describes the PostgreSQL Trigger and Rule Query Builder components.

## Trigger Commands

### CREATE TRIGGER

Create triggers using the `create_trigger()` DSL function:

```php
use function Flow\PgQuery\DSL\create;
use Flow\PgQuery\QueryBuilder\Schema\Trigger\TriggerEvent;

// Basic AFTER INSERT trigger
$builder = create()->trigger('audit_trigger')
    ->after(TriggerEvent::INSERT)
    ->on('users')
    ->execute('audit_function');
// CREATE TRIGGER audit_trigger AFTER INSERT ON users EXECUTE FUNCTION audit_function()

// Multiple events with FOR EACH ROW
$builder = create()->trigger('changes_trigger')
    ->after(TriggerEvent::INSERT, TriggerEvent::UPDATE, TriggerEvent::DELETE)
    ->on('orders')
    ->forEachRow()
    ->execute('track_changes');
// CREATE TRIGGER changes_trigger AFTER INSERT OR UPDATE OR DELETE ON orders FOR EACH ROW EXECUTE FUNCTION track_changes()

// BEFORE trigger on specific columns
$builder = create()->trigger('track_status')
    ->beforeUpdateOf('status', 'priority')
    ->on('tickets')
    ->forEachRow()
    ->execute('log_status_change');
// CREATE TRIGGER track_status BEFORE UPDATE OF status, priority ON tickets FOR EACH ROW EXECUTE FUNCTION log_status_change()

// INSTEAD OF trigger (for views)
$builder = create()->trigger('view_insert')
    ->insteadOf(TriggerEvent::INSERT)
    ->on('users_view')
    ->forEachRow()
    ->execute('handle_insert');
// CREATE TRIGGER view_insert INSTEAD OF INSERT ON users_view FOR EACH ROW EXECUTE FUNCTION handle_insert()

// With WHEN condition
$builder = create()->trigger('protect_admin')
    ->before(TriggerEvent::DELETE)
    ->on('users')
    ->forEachRow()
    ->when(raw_cond('OLD.role = \'admin\''))
    ->execute('raise_error');
// CREATE TRIGGER protect_admin BEFORE DELETE ON users FOR EACH ROW WHEN (old.role = 'admin') EXECUTE FUNCTION raise_error()

// OR REPLACE
$builder = create()->trigger('my_trigger')
    ->orReplace()
    ->after(TriggerEvent::INSERT)
    ->on('users')
    ->execute('my_function');
// CREATE OR REPLACE TRIGGER my_trigger AFTER INSERT ON users EXECUTE FUNCTION my_function()

// Constraint trigger with deferrable
$builder = create()->trigger('fk_check')
    ->constraint()
    ->after(TriggerEvent::INSERT)
    ->on('orders')
    ->from('users')
    ->deferrable()
    ->initiallyDeferred()
    ->forEachRow()
    ->execute('check_foreign_key');
// CREATE CONSTRAINT TRIGGER fk_check AFTER INSERT ON orders FROM users DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_foreign_key()

// With REFERENCING (transition tables)
$builder = create()->trigger('batch_process')
    ->after(TriggerEvent::INSERT)
    ->on('events')
    ->referencingNewTableAs('new_events')
    ->referencingOldTableAs('old_events')
    ->forEachStatement()
    ->execute('process_batch');
// CREATE TRIGGER batch_process AFTER INSERT ON events REFERENCING NEW TABLE new_events OLD TABLE old_events EXECUTE FUNCTION process_batch()
```

### ALTER TRIGGER

Rename triggers or manage extension dependencies:

```php
use function Flow\PgQuery\DSL\alter;

// Rename trigger
$builder = alter()->trigger('old_name')
    ->on('users')
    ->renameTo('new_name');
// ALTER TRIGGER old_name ON users RENAME TO new_name

// Depends on extension
$builder = alter()->trigger('my_trigger')
    ->on('users')
    ->dependsOnExtension('my_extension');
// ALTER TRIGGER my_trigger ON users DEPENDS ON EXTENSION my_extension

// Remove dependency on extension
$builder = alter()->trigger('my_trigger')
    ->on('users')
    ->noDependsOnExtension('my_extension');
// ALTER TRIGGER my_trigger ON users NO DEPENDS ON EXTENSION my_extension
```

### DROP TRIGGER

Drop triggers with optional IF EXISTS and CASCADE/RESTRICT:

```php
use function Flow\PgQuery\DSL\drop;

// Basic drop
$builder = drop()->trigger('audit_trigger')->on('users');
// DROP TRIGGER audit_trigger ON users

// With IF EXISTS
$builder = drop()->trigger('audit_trigger')
    ->ifExists()
    ->on('users');
// DROP TRIGGER IF EXISTS audit_trigger ON users

// With CASCADE
$builder = drop()->trigger('audit_trigger')
    ->on('users')
    ->cascade();
// DROP TRIGGER audit_trigger ON users CASCADE

// With schema
$builder = drop()->trigger('audit_trigger')
    ->on('public.users')
    ->restrict();
// DROP TRIGGER audit_trigger ON public.users
```

### ENABLE/DISABLE TRIGGER

Use `alter_table()` to enable or disable triggers:

```php
use function Flow\PgQuery\DSL\alter_table;

// Enable trigger
$builder = alter_table('users')->enableTrigger('audit_trigger');
// ALTER TABLE users ENABLE TRIGGER audit_trigger

// Enable all triggers
$builder = alter_table('users')->enableTriggerAll();
// ALTER TABLE users ENABLE TRIGGER ALL

// Enable user triggers only
$builder = alter_table('users')->enableTriggerUser();
// ALTER TABLE users ENABLE TRIGGER USER

// Enable always (fires even during replication)
$builder = alter_table('users')->enableTriggerAlways('critical_trigger');
// ALTER TABLE users ENABLE ALWAYS TRIGGER critical_trigger

// Enable replica (fires only during replication)
$builder = alter_table('users')->enableTriggerReplica('sync_trigger');
// ALTER TABLE users ENABLE REPLICA TRIGGER sync_trigger

// Disable trigger
$builder = alter_table('users')->disableTrigger('audit_trigger');
// ALTER TABLE users DISABLE TRIGGER audit_trigger

// Disable all triggers
$builder = alter_table('users')->disableTriggerAll();
// ALTER TABLE users DISABLE TRIGGER ALL

// Disable user triggers only
$builder = alter_table('users')->disableTriggerUser();
// ALTER TABLE users DISABLE TRIGGER USER
```

## Rule Commands

### CREATE RULE

Create rules using the `create_rule()` DSL function:

```php
use function Flow\PgQuery\DSL\create;

// DO NOTHING rule
$builder = create()->rule('prevent_delete')
    ->asOnDelete()
    ->to('users')
    ->doNothing();
// CREATE RULE prevent_delete AS ON DELETE TO users DO NOTHING

// DO INSTEAD rule
$builder = create()->rule('soft_delete')
    ->asOnDelete()
    ->to('users')
    ->doInstead("UPDATE users SET deleted = true WHERE id = OLD.id");
// CREATE RULE soft_delete AS ON DELETE TO users DO INSTEAD UPDATE users SET deleted = true WHERE id = old.id

// DO ALSO rule (additional action)
$builder = create()->rule('audit_insert')
    ->asOnInsert()
    ->to('orders')
    ->doAlso("INSERT INTO audit_log (action) VALUES ('insert')");
// CREATE RULE audit_insert AS ON INSERT TO orders DO INSERT INTO audit_log (action) VALUES ('insert')

// With WHERE condition
$builder = create()->rule('protect_admin')
    ->asOnDelete()
    ->to('users')
    ->where('OLD.role = \'admin\'')
    ->doNothing();
// CREATE RULE protect_admin AS ON DELETE TO users WHERE old.role = 'admin' DO NOTHING

// OR REPLACE
$builder = create()->rule('my_rule')
    ->orReplace()
    ->asOnUpdate()
    ->to('users')
    ->doNothing();
// CREATE OR REPLACE RULE my_rule AS ON UPDATE TO users DO NOTHING

// On SELECT (for views)
$builder = create()->rule('redirect_select')
    ->asOnSelect()
    ->to('old_view')
    ->doInstead('SELECT * FROM new_table');
// CREATE RULE redirect_select AS ON SELECT TO old_view DO INSTEAD SELECT * FROM new_table

// With schema
$builder = create()->rule('audit_rule')
    ->asOnInsert()
    ->to('public.users')
    ->doAlso("INSERT INTO audit VALUES ('insert')");
// CREATE RULE audit_rule AS ON INSERT TO public.users DO INSERT INTO audit VALUES ('insert')
```

### DROP RULE

Drop rules with optional IF EXISTS and CASCADE/RESTRICT:

```php
use function Flow\PgQuery\DSL\drop;

// Basic drop
$builder = drop()->rule('prevent_delete')->on('users');
// DROP RULE prevent_delete ON users

// With IF EXISTS
$builder = drop()->rule('prevent_delete')
    ->ifExists()
    ->on('users');
// DROP RULE IF EXISTS prevent_delete ON users

// With CASCADE
$builder = drop()->rule('prevent_delete')
    ->on('users')
    ->cascade();
// DROP RULE prevent_delete ON users CASCADE

// With schema
$builder = drop()->rule('audit_rule')
    ->on('public.users')
    ->restrict();
// DROP RULE audit_rule ON public.users
```

## Enums

### TriggerTiming

```php
use Flow\PgQuery\QueryBuilder\Schema\Trigger\TriggerTiming;

TriggerTiming::BEFORE;      // BEFORE timing
TriggerTiming::AFTER;       // AFTER timing
TriggerTiming::INSTEAD_OF;  // INSTEAD OF timing (for views)
```

### TriggerEvent

```php
use Flow\PgQuery\QueryBuilder\Schema\Trigger\TriggerEvent;

TriggerEvent::INSERT;    // INSERT event
TriggerEvent::UPDATE;    // UPDATE event
TriggerEvent::DELETE;    // DELETE event
TriggerEvent::TRUNCATE;  // TRUNCATE event
```

### TriggerLevel

```php
use Flow\PgQuery\QueryBuilder\Schema\Trigger\TriggerLevel;

TriggerLevel::ROW;        // FOR EACH ROW
TriggerLevel::STATEMENT;  // FOR EACH STATEMENT (default)
```

### RuleEvent

```php
use Flow\PgQuery\QueryBuilder\Schema\Rule\RuleEvent;

RuleEvent::SELECT;  // ON SELECT
RuleEvent::INSERT;  // ON INSERT
RuleEvent::UPDATE;  // ON UPDATE
RuleEvent::DELETE;  // ON DELETE
```

### RuleAction

```php
use Flow\PgQuery\QueryBuilder\Schema\Rule\RuleAction;

RuleAction::ALSO;     // DO ALSO
RuleAction::INSTEAD;  // DO INSTEAD
```
