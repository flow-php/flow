<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    alter,
    column,
    create,
    drop,
    insert,
    literal,
    primary_key,
    raw_cond,
    select,
    sql_type_integer,
    sql_type_serial,
    sql_type_varchar,
    star,
    table
};

use Flow\PostgreSql\QueryBuilder\Schema\DataType;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\TriggerEvent;

final class TriggerDatabaseTest extends DatabaseTestCase
{
    private const FUNCTION_NAME = 'flow_postgres_trigger_func';

    private const TABLE_LOG = 'flow_postgres_trigger_log';

    private const TABLE_NAME = 'flow_postgres_trigger_test';

    private const TRIGGER_NAME = 'flow_postgres_test_trigger';

    private const TRIGGER_NAME_RENAMED = 'flow_postgres_test_trigger_renamed';

    protected function setUp() : void
    {
        parent::setUp();

        $this->execute(
            create()->table(self::TABLE_NAME)
                ->column(column('id', sql_type_serial()))
                ->column(column('name', sql_type_varchar(100))->notNull())
                ->column(column('value', sql_type_integer())->default(0))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            create()->table(self::TABLE_LOG)
                ->column(column('id', sql_type_serial()))
                ->column(column('action', sql_type_varchar(50))->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(DataType::custom('trigger'))
                ->language('plpgsql')
                ->as('BEGIN INSERT INTO ' . self::TABLE_LOG . ' (action) VALUES (TG_OP); RETURN NEW; END;')
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->dropTriggerIfExists(self::TRIGGER_NAME, self::TABLE_NAME);
        $this->dropTriggerIfExists(self::TRIGGER_NAME_RENAMED, self::TABLE_NAME);
        $this->dropFunctionIfExists(self::FUNCTION_NAME);
        $this->dropTableIfExists(self::TABLE_LOG);
        $this->dropTableIfExists(self::TABLE_NAME);

        parent::tearDown();
    }

    public function test_alter_trigger_rename() : void
    {
        $this->execute(
            create()->trigger(self::TRIGGER_NAME)
                ->before(TriggerEvent::INSERT)
                ->on(self::TABLE_NAME)
                ->forEachRow()
                ->execute(self::FUNCTION_NAME)
                ->toSql()
        );

        $result = $this->execute(
            alter()->trigger(self::TRIGGER_NAME)
                ->on(self::TABLE_NAME)
                ->renameTo(self::TRIGGER_NAME_RENAMED)
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));
        self::assertTrue($this->triggerExists(self::TRIGGER_NAME_RENAMED, self::TABLE_NAME));
    }

    public function test_create_or_replace_trigger() : void
    {
        $this->execute(
            create()->trigger(self::TRIGGER_NAME)
                ->before(TriggerEvent::INSERT)
                ->on(self::TABLE_NAME)
                ->forEachRow()
                ->execute(self::FUNCTION_NAME)
                ->toSql()
        );

        $result = $this->execute(
            create()->trigger(self::TRIGGER_NAME)
                ->orReplace()
                ->after(TriggerEvent::INSERT)
                ->on(self::TABLE_NAME)
                ->forEachRow()
                ->execute(self::FUNCTION_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));
    }

    public function test_create_trigger_after_insert() : void
    {
        $result = $this->execute(
            create()->trigger(self::TRIGGER_NAME)
                ->after(TriggerEvent::INSERT)
                ->on(self::TABLE_NAME)
                ->forEachRow()
                ->execute(self::FUNCTION_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));
    }

    public function test_create_trigger_after_multiple_events() : void
    {
        $result = $this->execute(
            create()->trigger(self::TRIGGER_NAME)
                ->after(TriggerEvent::INSERT, TriggerEvent::UPDATE)
                ->on(self::TABLE_NAME)
                ->forEachRow()
                ->execute(self::FUNCTION_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));
    }

    public function test_create_trigger_before_insert() : void
    {
        $result = $this->execute(
            create()->trigger(self::TRIGGER_NAME)
                ->before(TriggerEvent::INSERT)
                ->on(self::TABLE_NAME)
                ->forEachRow()
                ->execute(self::FUNCTION_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));

        $this->execute(
            insert()
                ->into(self::TABLE_NAME)
                ->columns('name')
                ->values(literal('Test'))
                ->toSql()
        );

        $logs = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_LOG))
                    ->toSql()
            )
        );

        self::assertCount(1, $logs);
        self::assertSame('INSERT', $logs[0]['action']);
    }

    public function test_create_trigger_with_when_condition() : void
    {
        $result = $this->execute(
            create()->trigger(self::TRIGGER_NAME)
                ->before(TriggerEvent::INSERT)
                ->on(self::TABLE_NAME)
                ->forEachRow()
                ->when(raw_cond('NEW.value <> 0'))
                ->execute(self::FUNCTION_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));

        $this->execute(
            insert()
                ->into(self::TABLE_NAME)
                ->columns('name', 'value')
                ->values(literal('Zero'), literal(0))
                ->toSql()
        );

        $logs = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_LOG))
                    ->toSql()
            )
        );
        self::assertCount(0, $logs);

        $this->execute(
            insert()
                ->into(self::TABLE_NAME)
                ->columns('name', 'value')
                ->values(literal('NonZero'), literal(5))
                ->toSql()
        );

        $logs = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_LOG))
                    ->toSql()
            )
        );
        self::assertCount(1, $logs);
    }

    public function test_drop_trigger() : void
    {
        $this->execute(
            create()->trigger(self::TRIGGER_NAME)
                ->before(TriggerEvent::INSERT)
                ->on(self::TABLE_NAME)
                ->forEachRow()
                ->execute(self::FUNCTION_NAME)
                ->toSql()
        );

        self::assertTrue($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));

        $result = $this->execute(
            drop()->trigger(self::TRIGGER_NAME)->on(self::TABLE_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));
    }

    public function test_drop_trigger_cascade() : void
    {
        $this->execute(
            create()->trigger(self::TRIGGER_NAME)
                ->before(TriggerEvent::INSERT)
                ->on(self::TABLE_NAME)
                ->forEachRow()
                ->execute(self::FUNCTION_NAME)
                ->toSql()
        );

        $result = $this->execute(
            drop()->trigger(self::TRIGGER_NAME)->on(self::TABLE_NAME)->cascade()
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->triggerExists(self::TRIGGER_NAME, self::TABLE_NAME));
    }

    public function test_drop_trigger_if_exists() : void
    {
        $result = $this->execute(
            drop()->trigger(self::TRIGGER_NAME)->ifExists()->on(self::TABLE_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    protected function dropFunctionIfExists(string $name) : void
    {
        $this->execute("DROP FUNCTION IF EXISTS {$name} CASCADE");
    }

    protected function dropTriggerIfExists(string $triggerName, string $tableName) : void
    {
        $this->execute("DROP TRIGGER IF EXISTS {$triggerName} ON {$tableName} CASCADE");
    }

    protected function triggerExists(string $triggerName, string $tableName) : bool
    {
        $row = $this->fetchOne(
            $this->execute(
                "SELECT EXISTS(
                    SELECT 1 FROM pg_trigger t
                    JOIN pg_class c ON t.tgrelid = c.oid
                    WHERE t.tgname = '{$triggerName}'
                    AND c.relname = '{$tableName}'
                    AND NOT t.tgisinternal
                ) AS trigger_exists"
            )
        );

        return $row['trigger_exists'] === 't';
    }
}
