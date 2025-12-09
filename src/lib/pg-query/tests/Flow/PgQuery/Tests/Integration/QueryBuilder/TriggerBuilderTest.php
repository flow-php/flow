<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{alter_table, alter_trigger, create_trigger, drop_trigger, raw_cond};
use Flow\PgQuery\QueryBuilder\Schema\Trigger\TriggerEvent;
use Flow\PgQuery\Tests\Integration\QueryBuilder\Assertions\QueryBuilderAssertions;
use PHPUnit\Framework\TestCase;

final class TriggerBuilderTest extends TestCase
{
    use QueryBuilderAssertions;

    public function test_alter_table_disable_trigger() : void
    {
        $builder = alter_table('users')
            ->disableTrigger('audit_trigger');

        $this->assertAlterTableQuery($builder, 'ALTER TABLE users DISABLE TRIGGER audit_trigger');
    }

    public function test_alter_table_disable_trigger_all() : void
    {
        $builder = alter_table('users')
            ->disableTriggerAll();

        $this->assertAlterTableQuery($builder, 'ALTER TABLE users DISABLE TRIGGER ALL');
    }

    public function test_alter_table_disable_trigger_user() : void
    {
        $builder = alter_table('users')
            ->disableTriggerUser();

        $this->assertAlterTableQuery($builder, 'ALTER TABLE users DISABLE TRIGGER USER');
    }

    public function test_alter_table_enable_trigger() : void
    {
        $builder = alter_table('users')
            ->enableTrigger('audit_trigger');

        $this->assertAlterTableQuery($builder, 'ALTER TABLE users ENABLE TRIGGER audit_trigger');
    }

    public function test_alter_table_enable_trigger_all() : void
    {
        $builder = alter_table('users')
            ->enableTriggerAll();

        $this->assertAlterTableQuery($builder, 'ALTER TABLE users ENABLE TRIGGER ALL');
    }

    public function test_alter_table_enable_trigger_always() : void
    {
        $builder = alter_table('users')
            ->enableTriggerAlways('audit_trigger');

        $this->assertAlterTableQuery($builder, 'ALTER TABLE users ENABLE ALWAYS TRIGGER audit_trigger');
    }

    public function test_alter_table_enable_trigger_replica() : void
    {
        $builder = alter_table('users')
            ->enableTriggerReplica('audit_trigger');

        $this->assertAlterTableQuery($builder, 'ALTER TABLE users ENABLE REPLICA TRIGGER audit_trigger');
    }

    public function test_alter_table_enable_trigger_user() : void
    {
        $builder = alter_table('users')
            ->enableTriggerUser();

        $this->assertAlterTableQuery($builder, 'ALTER TABLE users ENABLE TRIGGER USER');
    }

    public function test_alter_trigger_depends_on_extension() : void
    {
        $builder = alter_trigger('my_trigger')
            ->on('users')
            ->dependsOnExtension('my_extension');

        $this->assertAlterTriggerDependsQuery($builder, 'ALTER TRIGGER my_trigger ON users DEPENDS ON EXTENSION my_extension');
    }

    public function test_alter_trigger_no_depends_on_extension() : void
    {
        $builder = alter_trigger('my_trigger')
            ->on('users')
            ->noDependsOnExtension('my_extension');

        $this->assertAlterTriggerDependsQuery($builder, 'ALTER TRIGGER my_trigger ON users NO DEPENDS ON EXTENSION my_extension');
    }

    public function test_alter_trigger_rename() : void
    {
        $builder = alter_trigger('old_trigger')
            ->on('users')
            ->renameTo('new_trigger');

        $this->assertAlterTriggerRenameQuery($builder, 'ALTER TRIGGER old_trigger ON users RENAME TO new_trigger');
    }

    public function test_create_trigger_after_insert() : void
    {
        $builder = create_trigger('audit_trigger')
            ->after(TriggerEvent::INSERT)
            ->on('users')
            ->execute('audit_function');

        $this->assertCreateTriggerQuery($builder, 'CREATE TRIGGER audit_trigger AFTER INSERT ON users EXECUTE FUNCTION audit_function()');
    }

    public function test_create_trigger_after_insert_or_update() : void
    {
        $builder = create_trigger('audit_trigger')
            ->after(TriggerEvent::INSERT, TriggerEvent::UPDATE)
            ->on('users')
            ->forEachRow()
            ->execute('audit_function');

        $this->assertCreateTriggerQuery($builder, 'CREATE TRIGGER audit_trigger AFTER INSERT OR UPDATE ON users FOR EACH ROW EXECUTE FUNCTION audit_function()');
    }

    public function test_create_trigger_after_update_of_columns() : void
    {
        $builder = create_trigger('track_changes')
            ->afterUpdateOf('status', 'updated_at')
            ->on('orders')
            ->forEachRow()
            ->execute('track_changes_function');

        $this->assertCreateTriggerQuery($builder, 'CREATE TRIGGER track_changes AFTER UPDATE OF status, updated_at ON orders FOR EACH ROW EXECUTE FUNCTION track_changes_function()');
    }

    public function test_create_trigger_before_delete_with_when() : void
    {
        $builder = create_trigger('prevent_delete')
            ->before(TriggerEvent::DELETE)
            ->on('users')
            ->forEachRow()
            ->when(raw_cond('OLD.protected = true'))
            ->execute('raise_exception');

        $this->assertCreateTriggerQuery($builder, 'CREATE TRIGGER prevent_delete BEFORE DELETE ON users FOR EACH ROW WHEN (old.protected = true) EXECUTE FUNCTION raise_exception()');
    }

    public function test_create_trigger_constraint_deferrable() : void
    {
        $builder = create_trigger('fk_trigger')
            ->constraint()
            ->after(TriggerEvent::INSERT)
            ->on('orders')
            ->from('users')
            ->deferrable()
            ->initiallyDeferred()
            ->forEachRow()
            ->execute('check_fk');

        $this->assertCreateTriggerQuery($builder, 'CREATE CONSTRAINT TRIGGER fk_trigger AFTER INSERT ON orders FROM users DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_fk()');
    }

    public function test_create_trigger_instead_of_on_view() : void
    {
        $builder = create_trigger('view_insert')
            ->insteadOf(TriggerEvent::INSERT)
            ->on('users_view')
            ->forEachRow()
            ->execute('insert_to_users');

        $this->assertCreateTriggerQuery($builder, 'CREATE TRIGGER view_insert INSTEAD OF INSERT ON users_view FOR EACH ROW EXECUTE FUNCTION insert_to_users()');
    }

    public function test_create_trigger_or_replace() : void
    {
        $builder = create_trigger('audit_trigger')
            ->orReplace()
            ->after(TriggerEvent::INSERT)
            ->on('users')
            ->execute('audit_function');

        $this->assertCreateTriggerQuery($builder, 'CREATE OR REPLACE TRIGGER audit_trigger AFTER INSERT ON users EXECUTE FUNCTION audit_function()');
    }

    public function test_create_trigger_with_referencing() : void
    {
        $builder = create_trigger('statement_trigger')
            ->after(TriggerEvent::INSERT)
            ->on('users')
            ->referencingNewTableAs('inserted_rows')
            ->execute('process_batch');

        $this->assertCreateTriggerQuery($builder, 'CREATE TRIGGER statement_trigger AFTER INSERT ON users REFERENCING NEW TABLE inserted_rows EXECUTE FUNCTION process_batch()');
    }

    public function test_create_trigger_with_schema() : void
    {
        $builder = create_trigger('audit_trigger')
            ->after(TriggerEvent::INSERT)
            ->on('public.users')
            ->execute('audit_function');

        $this->assertCreateTriggerQuery($builder, 'CREATE TRIGGER audit_trigger AFTER INSERT ON public.users EXECUTE FUNCTION audit_function()');
    }

    public function test_drop_trigger() : void
    {
        $builder = drop_trigger('audit_trigger')
            ->on('users');

        $this->assertDropTriggerQuery($builder, 'DROP TRIGGER audit_trigger ON users');
    }

    public function test_drop_trigger_cascade() : void
    {
        $builder = drop_trigger('audit_trigger')
            ->on('users')
            ->cascade();

        $this->assertDropTriggerQuery($builder, 'DROP TRIGGER audit_trigger ON users CASCADE');
    }

    public function test_drop_trigger_if_exists() : void
    {
        $builder = drop_trigger('audit_trigger')
            ->ifExists()
            ->on('users');

        $this->assertDropTriggerQuery($builder, 'DROP TRIGGER IF EXISTS audit_trigger ON users');
    }

    public function test_drop_trigger_with_schema() : void
    {
        $builder = drop_trigger('audit_trigger')
            ->on('public.users')
            ->restrict();

        $this->assertDropTriggerQuery($builder, 'DROP TRIGGER audit_trigger ON public.users');
    }
}
