<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder\Database;

use function Flow\PgQuery\DSL\{
    alter_schema,
    column,
    create_schema,
    create_table,
    drop_schema,
    insert,
    literal_string,
    primary_key,
    select,
    sql_type_serial,
    sql_type_varchar,
    star,
    table
};

final class SchemaDatabaseTest extends DatabaseTestCase
{
    private const SCHEMA_NAME = 'flow_postgres_test_schema';

    private const SCHEMA_NAME_RENAMED = 'flow_postgres_test_schema_renamed';

    private const TABLE_NAME = 'flow_postgres_schema_table';

    protected function setUp() : void
    {
        parent::setUp();

        $this->dropSchemaIfExists(self::SCHEMA_NAME);
        $this->dropSchemaIfExists(self::SCHEMA_NAME_RENAMED);
    }

    protected function tearDown() : void
    {
        $this->dropSchemaIfExists(self::SCHEMA_NAME);
        $this->dropSchemaIfExists(self::SCHEMA_NAME_RENAMED);

        parent::tearDown();
    }

    public function test_alter_schema_owner() : void
    {
        $this->execute(
            create_schema(self::SCHEMA_NAME)->toSql()
        );

        $currentUser = $this->fetchOne($this->execute('SELECT current_user AS username'));
        self::assertIsString($currentUser['username']);

        $result = $this->execute(
            alter_schema(self::SCHEMA_NAME)
                ->ownerTo($currentUser['username'])
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_alter_schema_rename() : void
    {
        $this->execute(
            create_schema(self::SCHEMA_NAME)->toSql()
        );

        $result = $this->execute(
            alter_schema(self::SCHEMA_NAME)
                ->renameTo(self::SCHEMA_NAME_RENAMED)
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->schemaExists(self::SCHEMA_NAME));
        self::assertTrue($this->schemaExists(self::SCHEMA_NAME_RENAMED));
    }

    public function test_create_schema() : void
    {
        $result = $this->execute(
            create_schema(self::SCHEMA_NAME)->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->schemaExists(self::SCHEMA_NAME));
    }

    public function test_create_schema_if_not_exists() : void
    {
        $this->execute(
            create_schema(self::SCHEMA_NAME)->toSql()
        );

        $result = $this->execute(
            create_schema(self::SCHEMA_NAME)->ifNotExists()->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->schemaExists(self::SCHEMA_NAME));
    }

    public function test_create_schema_with_authorization() : void
    {
        $currentUser = $this->fetchOne($this->execute('SELECT current_user AS username'));
        self::assertIsString($currentUser['username']);

        $result = $this->execute(
            create_schema(self::SCHEMA_NAME)
                ->authorization($currentUser['username'])
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->schemaExists(self::SCHEMA_NAME));
    }

    public function test_create_table_in_schema() : void
    {
        $this->execute(
            create_schema(self::SCHEMA_NAME)->toSql()
        );

        $result = $this->execute(
            create_table(self::TABLE_NAME, self::SCHEMA_NAME)
                ->column(column('id', sql_type_serial()))
                ->column(column('name', sql_type_varchar(100))->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        self::assertNotFalse($result);

        $this->execute(
            insert()
                ->into(self::SCHEMA_NAME . '.' . self::TABLE_NAME)
                ->columns('name')
                ->values(literal_string('Test'))
                ->toSql()
        );

        $row = $this->fetchOne(
            $this->execute(
                select(star())->from(table(self::TABLE_NAME, self::SCHEMA_NAME))->toSql()
            )
        );

        self::assertSame('Test', $row['name']);
    }

    public function test_drop_multiple_schemas() : void
    {
        $this->execute(create_schema(self::SCHEMA_NAME)->toSql());
        $this->execute(create_schema(self::SCHEMA_NAME_RENAMED)->toSql());

        self::assertTrue($this->schemaExists(self::SCHEMA_NAME));
        self::assertTrue($this->schemaExists(self::SCHEMA_NAME_RENAMED));

        $result = $this->execute(
            drop_schema(self::SCHEMA_NAME, self::SCHEMA_NAME_RENAMED)->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->schemaExists(self::SCHEMA_NAME));
        self::assertFalse($this->schemaExists(self::SCHEMA_NAME_RENAMED));
    }

    public function test_drop_schema() : void
    {
        $this->execute(
            create_schema(self::SCHEMA_NAME)->toSql()
        );

        self::assertTrue($this->schemaExists(self::SCHEMA_NAME));

        $result = $this->execute(
            drop_schema(self::SCHEMA_NAME)->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->schemaExists(self::SCHEMA_NAME));
    }

    public function test_drop_schema_cascade() : void
    {
        $this->execute(
            create_schema(self::SCHEMA_NAME)->toSql()
        );

        $this->execute(
            create_table(self::TABLE_NAME, self::SCHEMA_NAME)
                ->column(column('id', sql_type_serial()))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $result = $this->execute(
            drop_schema(self::SCHEMA_NAME)->cascade()->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->schemaExists(self::SCHEMA_NAME));
    }

    public function test_drop_schema_if_exists() : void
    {
        $result = $this->execute(
            drop_schema(self::SCHEMA_NAME)->ifExists()->toSql()
        );

        self::assertNotFalse($result);
    }

    protected function dropSchemaIfExists(string $name) : void
    {
        $this->execute("DROP SCHEMA IF EXISTS {$name} CASCADE");
    }

    protected function schemaExists(string $name) : bool
    {
        $row = $this->fetchOne(
            $this->execute("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '{$name}') AS schema_exists")
        );

        return $row['schema_exists'] === 't';
    }
}
