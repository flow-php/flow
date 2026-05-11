<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\TriggerEvent;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_serial;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\ne;
use function Flow\PostgreSql\DSL\primary_key;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;

final class TriggerDatabaseTest extends PostgreSqlTestCase
{
    private const FUNCTION_NAME = 'flow_postgres_trigger_func';

    private const TABLE_LOG = 'flow_postgres_trigger_log';

    private const TABLE_NAME = 'flow_postgres_trigger_test';

    private const TRIGGER_NAME = 'flow_postgres_test_trigger';

    private const TRIGGER_NAME_RENAMED = 'flow_postgres_test_trigger_renamed';

    protected function setUp(): void
    {
        parent::setUp();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_NAME)
                    ->column(column('id', column_type_serial()))
                    ->column(column('name', column_type_varchar(100))->notNull())
                    ->column(column('value', column_type_integer())->default(0))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_LOG)
                    ->column(column('id', column_type_serial()))
                    ->column(column('action', column_type_varchar(50))->notNull())
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->function(self::FUNCTION_NAME)
                    ->arguments()
                    ->returns(ColumnType::custom('trigger'))
                    ->language('plpgsql')
                    ->as('BEGIN INSERT INTO ' . self::TABLE_LOG . ' (action) VALUES (TG_OP); RETURN NEW; END;')
                    ->toSql(),
            );
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropTriggerIfExists(self::TRIGGER_NAME, self::TABLE_NAME);
        $this->pgsqlContext()->dropTriggerIfExists(self::TRIGGER_NAME_RENAMED, self::TABLE_NAME);
        $this->pgsqlContext()->dropFunctionIfExists(self::FUNCTION_NAME);
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_LOG);
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_NAME);

        parent::tearDown();
    }

    public function test_alter_trigger_rename(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->trigger(self::TRIGGER_NAME)
                    ->before(TriggerEvent::INSERT)
                    ->on(self::TABLE_NAME)
                    ->forEachRow()
                    ->execute(self::FUNCTION_NAME)
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                alter()
                    ->trigger(self::TRIGGER_NAME)
                    ->on(self::TABLE_NAME)
                    ->renameTo(self::TRIGGER_NAME_RENAMED)
                    ->toSql(),
            );

        static::assertFalse($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));
        static::assertTrue($this->triggerExists(self::TRIGGER_NAME_RENAMED, self::TABLE_NAME));
    }

    public function test_create_or_replace_trigger(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->trigger(self::TRIGGER_NAME)
                    ->before(TriggerEvent::INSERT)
                    ->on(self::TABLE_NAME)
                    ->forEachRow()
                    ->execute(self::FUNCTION_NAME)
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->trigger(self::TRIGGER_NAME)
                    ->orReplace()
                    ->after(TriggerEvent::INSERT)
                    ->on(self::TABLE_NAME)
                    ->forEachRow()
                    ->execute(self::FUNCTION_NAME)
                    ->toSql(),
            );

        static::assertTrue($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));
    }

    public function test_create_trigger_after_insert(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->trigger(self::TRIGGER_NAME)
                    ->after(TriggerEvent::INSERT)
                    ->on(self::TABLE_NAME)
                    ->forEachRow()
                    ->execute(self::FUNCTION_NAME)
                    ->toSql(),
            );

        static::assertTrue($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));
    }

    public function test_create_trigger_after_multiple_events(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->trigger(self::TRIGGER_NAME)
                    ->after(TriggerEvent::INSERT, TriggerEvent::UPDATE)
                    ->on(self::TABLE_NAME)
                    ->forEachRow()
                    ->execute(self::FUNCTION_NAME)
                    ->toSql(),
            );

        static::assertTrue($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));
    }

    public function test_create_trigger_before_insert(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->trigger(self::TRIGGER_NAME)
                    ->before(TriggerEvent::INSERT)
                    ->on(self::TABLE_NAME)
                    ->forEachRow()
                    ->execute(self::FUNCTION_NAME)
                    ->toSql(),
            );

        static::assertTrue($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));

        $this
            ->pgsqlContext()
            ->client()
            ->execute(insert()->into(self::TABLE_NAME)->columns('name')->values(literal('Test'))->toSql());

        $logs = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll(select(star())->from(table(self::TABLE_LOG))->toSql());

        static::assertCount(1, $logs);
        static::assertSame('INSERT', $logs[0]['action']);
    }

    public function test_create_trigger_with_when_condition(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->trigger(self::TRIGGER_NAME)
                    ->before(TriggerEvent::INSERT)
                    ->on(self::TABLE_NAME)
                    ->forEachRow()
                    ->when(ne(col('value', 'new'), literal(0)))
                    ->execute(self::FUNCTION_NAME)
                    ->toSql(),
            );

        static::assertTrue($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::TABLE_NAME)
                    ->columns('name', 'value')
                    ->values(literal('Zero'), literal(0))
                    ->toSql(),
            );

        $logs = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll(select(star())->from(table(self::TABLE_LOG))->toSql());
        static::assertCount(0, $logs);

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::TABLE_NAME)
                    ->columns('name', 'value')
                    ->values(literal('NonZero'), literal(5))
                    ->toSql(),
            );

        $logs = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll(select(star())->from(table(self::TABLE_LOG))->toSql());
        static::assertCount(1, $logs);
    }

    public function test_drop_trigger(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->trigger(self::TRIGGER_NAME)
                    ->before(TriggerEvent::INSERT)
                    ->on(self::TABLE_NAME)
                    ->forEachRow()
                    ->execute(self::FUNCTION_NAME)
                    ->toSql(),
            );

        static::assertTrue($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));

        $this->pgsqlContext()->client()->execute(drop()->trigger(self::TRIGGER_NAME)->on(self::TABLE_NAME)->toSql());

        static::assertFalse($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));
    }

    public function test_drop_trigger_cascade(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->trigger(self::TRIGGER_NAME)
                    ->before(TriggerEvent::INSERT)
                    ->on(self::TABLE_NAME)
                    ->forEachRow()
                    ->execute(self::FUNCTION_NAME)
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(drop()->trigger(self::TRIGGER_NAME)->on(self::TABLE_NAME)->cascade()->toSql());

        static::assertFalse($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));
    }

    public function test_drop_trigger_if_exists(): void
    {
        $this->expectNotToPerformAssertions();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(drop()->trigger(self::TRIGGER_NAME)->ifExists()->on(self::TABLE_NAME)->toSql());
    }

    protected function triggerExists(string $triggerName, string $tableName): bool
    {
        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle("SELECT EXISTS(
                    SELECT 1 FROM pg_trigger t
                    JOIN pg_class c ON t.tgrelid = c.oid
                    WHERE t.tgname = '{$triggerName}'
                    AND c.relname = '{$tableName}'
                    AND NOT t.tgisinternal
                ) AS trigger_exists");

        return $row['trigger_exists'] === true;
    }
}
