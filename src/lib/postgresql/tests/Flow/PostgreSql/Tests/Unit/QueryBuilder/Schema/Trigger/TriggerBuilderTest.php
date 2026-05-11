<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Trigger;

use Flow\PostgreSql\QueryBuilder\Schema\Trigger\TriggerEvent;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;

final class TriggerBuilderTest extends TestCase
{
    public function test_alter_table_disable_trigger(): void
    {
        $builder = alter()->table('users')->disableTrigger('audit_trigger');

        static::assertSame('ALTER TABLE users DISABLE TRIGGER audit_trigger', $builder->toSql());
    }

    public function test_alter_table_disable_trigger_all(): void
    {
        $builder = alter()->table('users')->disableTriggerAll();

        static::assertSame('ALTER TABLE users DISABLE TRIGGER ALL', $builder->toSql());
    }

    public function test_alter_table_disable_trigger_user(): void
    {
        $builder = alter()->table('users')->disableTriggerUser();

        static::assertSame('ALTER TABLE users DISABLE TRIGGER USER', $builder->toSql());
    }

    public function test_alter_table_enable_trigger(): void
    {
        $builder = alter()->table('users')->enableTrigger('audit_trigger');

        static::assertSame('ALTER TABLE users ENABLE TRIGGER audit_trigger', $builder->toSql());
    }

    public function test_alter_table_enable_trigger_all(): void
    {
        $builder = alter()->table('users')->enableTriggerAll();

        static::assertSame('ALTER TABLE users ENABLE TRIGGER ALL', $builder->toSql());
    }

    public function test_alter_table_enable_trigger_always(): void
    {
        $builder = alter()->table('users')->enableTriggerAlways('audit_trigger');

        static::assertSame('ALTER TABLE users ENABLE ALWAYS TRIGGER audit_trigger', $builder->toSql());
    }

    public function test_alter_table_enable_trigger_replica(): void
    {
        $builder = alter()->table('users')->enableTriggerReplica('audit_trigger');

        static::assertSame('ALTER TABLE users ENABLE REPLICA TRIGGER audit_trigger', $builder->toSql());
    }

    public function test_alter_table_enable_trigger_user(): void
    {
        $builder = alter()->table('users')->enableTriggerUser();

        static::assertSame('ALTER TABLE users ENABLE TRIGGER USER', $builder->toSql());
    }

    public function test_alter_trigger_depends_on_extension(): void
    {
        $builder = alter()->trigger('my_trigger')->on('users')->dependsOnExtension('my_extension');

        static::assertSame('ALTER TRIGGER my_trigger ON users DEPENDS ON EXTENSION my_extension', $builder->toSql());
    }

    public function test_alter_trigger_no_depends_on_extension(): void
    {
        $builder = alter()->trigger('my_trigger')->on('users')->noDependsOnExtension('my_extension');

        static::assertSame('ALTER TRIGGER my_trigger ON users NO DEPENDS ON EXTENSION my_extension', $builder->toSql());
    }

    public function test_alter_trigger_rename(): void
    {
        $builder = alter()->trigger('old_trigger')->on('users')->renameTo('new_trigger');

        static::assertSame('ALTER TRIGGER old_trigger ON users RENAME TO new_trigger', $builder->toSql());
    }

    public function test_create_trigger_after_insert(): void
    {
        $builder = create()
            ->trigger('audit_trigger')
            ->after(TriggerEvent::INSERT)
            ->on('users')
            ->execute('audit_function');

        static::assertSame(
            'CREATE TRIGGER audit_trigger AFTER INSERT ON users EXECUTE FUNCTION audit_function()',
            $builder->toSql(),
        );
    }

    public function test_create_trigger_after_insert_or_update(): void
    {
        $builder = create()
            ->trigger('audit_trigger')
            ->after(TriggerEvent::INSERT, TriggerEvent::UPDATE)
            ->on('users')
            ->forEachRow()
            ->execute('audit_function');

        static::assertSame(
            'CREATE TRIGGER audit_trigger AFTER INSERT OR UPDATE ON users FOR EACH ROW EXECUTE FUNCTION audit_function()',
            $builder->toSql(),
        );
    }

    public function test_create_trigger_after_update_of_columns(): void
    {
        $builder = create()
            ->trigger('track_changes')
            ->afterUpdateOf('status', 'updated_at')
            ->on('orders')
            ->forEachRow()
            ->execute('track_changes_function');

        static::assertSame(
            'CREATE TRIGGER track_changes AFTER UPDATE OF status, updated_at ON orders FOR EACH ROW EXECUTE FUNCTION track_changes_function()',
            $builder->toSql(),
        );
    }

    public function test_create_trigger_before_delete_with_when(): void
    {
        $builder = create()
            ->trigger('prevent_delete')
            ->before(TriggerEvent::DELETE)
            ->on('users')
            ->forEachRow()
            ->when(eq(col('protected', 'old'), literal(true)))
            ->execute('raise_exception');

        static::assertSame(
            'CREATE TRIGGER prevent_delete BEFORE DELETE ON users FOR EACH ROW WHEN (old.protected = true) EXECUTE FUNCTION raise_exception()',
            $builder->toSql(),
        );
    }

    public function test_create_trigger_constraint_deferrable(): void
    {
        $builder = create()
            ->trigger('fk_trigger')
            ->constraint()
            ->after(TriggerEvent::INSERT)
            ->on('orders')
            ->from('users')
            ->deferrable()
            ->initiallyDeferred()
            ->forEachRow()
            ->execute('check_fk');

        static::assertSame(
            'CREATE CONSTRAINT TRIGGER fk_trigger AFTER INSERT ON orders FROM users DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_fk()',
            $builder->toSql(),
        );
    }

    public function test_create_trigger_instead_of_on_view(): void
    {
        $builder = create()
            ->trigger('view_insert')
            ->insteadOf(TriggerEvent::INSERT)
            ->on('users_view')
            ->forEachRow()
            ->execute('insert_to_users');

        static::assertSame(
            'CREATE TRIGGER view_insert INSTEAD OF INSERT ON users_view FOR EACH ROW EXECUTE FUNCTION insert_to_users()',
            $builder->toSql(),
        );
    }

    public function test_create_trigger_or_replace(): void
    {
        $builder = create()
            ->trigger('audit_trigger')
            ->orReplace()
            ->after(TriggerEvent::INSERT)
            ->on('users')
            ->execute('audit_function');

        static::assertSame(
            'CREATE OR REPLACE TRIGGER audit_trigger AFTER INSERT ON users EXECUTE FUNCTION audit_function()',
            $builder->toSql(),
        );
    }

    public function test_create_trigger_with_referencing(): void
    {
        $builder = create()
            ->trigger('statement_trigger')
            ->after(TriggerEvent::INSERT)
            ->on('users')
            ->referencingNewTableAs('inserted_rows')
            ->execute('process_batch');

        static::assertSame(
            'CREATE TRIGGER statement_trigger AFTER INSERT ON users REFERENCING NEW TABLE inserted_rows EXECUTE FUNCTION process_batch()',
            $builder->toSql(),
        );
    }

    public function test_create_trigger_with_schema(): void
    {
        $builder = create()
            ->trigger('audit_trigger')
            ->after(TriggerEvent::INSERT)
            ->on('public.users')
            ->execute('audit_function');

        static::assertSame(
            'CREATE TRIGGER audit_trigger AFTER INSERT ON public.users EXECUTE FUNCTION audit_function()',
            $builder->toSql(),
        );
    }

    public function test_drop_trigger(): void
    {
        $builder = drop()->trigger('audit_trigger')->on('users');

        static::assertSame('DROP TRIGGER audit_trigger ON users', $builder->toSql());
    }

    public function test_drop_trigger_cascade(): void
    {
        $builder = drop()->trigger('audit_trigger')->on('users')->cascade();

        static::assertSame('DROP TRIGGER audit_trigger ON users CASCADE', $builder->toSql());
    }

    public function test_drop_trigger_if_exists(): void
    {
        $builder = drop()->trigger('audit_trigger')->ifExists()->on('users');

        static::assertSame('DROP TRIGGER IF EXISTS audit_trigger ON users', $builder->toSql());
    }

    public function test_drop_trigger_with_schema(): void
    {
        $builder = drop()->trigger('audit_trigger')->on('public.users')->restrict();

        static::assertSame('DROP TRIGGER audit_trigger ON public.users', $builder->toSql());
    }
}
