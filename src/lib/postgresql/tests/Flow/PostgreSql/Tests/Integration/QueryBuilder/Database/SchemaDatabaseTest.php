<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_serial;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\primary_key;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;

final class SchemaDatabaseTest extends PostgreSqlTestCase
{
    private const SCHEMA_NAME = 'flow_postgres_test_schema';

    private const SCHEMA_NAME_RENAMED = 'flow_postgres_test_schema_renamed';

    private const TABLE_NAME = 'flow_postgres_schema_table';

    protected function setUp(): void
    {
        parent::setUp();

        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA_NAME);
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA_NAME_RENAMED);
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA_NAME);
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA_NAME_RENAMED);

        parent::tearDown();
    }

    public function test_alter_schema_owner(): void
    {
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA_NAME)->toSql());

        $currentUser = $this->pgsqlContext()->client()->fetchSingle('SELECT current_user AS username');
        static::assertIsString($currentUser['username']);

        $this
            ->pgsqlContext()
            ->client()
            ->execute(alter()->schema(self::SCHEMA_NAME)->ownerTo($currentUser['username'])->toSql());
    }

    public function test_alter_schema_rename(): void
    {
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA_NAME)->toSql());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(alter()->schema(self::SCHEMA_NAME)->renameTo(self::SCHEMA_NAME_RENAMED)->toSql());

        static::assertFalse($this->schemaExists(self::SCHEMA_NAME));
        static::assertTrue($this->schemaExists(self::SCHEMA_NAME_RENAMED));
    }

    public function test_create_schema(): void
    {
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA_NAME)->toSql());

        static::assertTrue($this->schemaExists(self::SCHEMA_NAME));
    }

    public function test_create_schema_if_not_exists(): void
    {
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA_NAME)->toSql());

        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA_NAME)->ifNotExists()->toSql());

        static::assertTrue($this->schemaExists(self::SCHEMA_NAME));
    }

    public function test_create_schema_with_authorization(): void
    {
        $currentUser = $this->pgsqlContext()->client()->fetchSingle('SELECT current_user AS username');
        static::assertIsString($currentUser['username']);

        $this
            ->pgsqlContext()
            ->client()
            ->execute(create()->schema(self::SCHEMA_NAME)->authorization($currentUser['username'])->toSql());

        static::assertTrue($this->schemaExists(self::SCHEMA_NAME));
    }

    public function test_create_table_in_schema(): void
    {
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA_NAME)->toSql());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_NAME, self::SCHEMA_NAME)
                    ->column(column('id', column_type_serial()))
                    ->column(column('name', column_type_varchar(100))->notNull())
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::SCHEMA_NAME . '.' . self::TABLE_NAME)
                    ->columns('name')
                    ->values(literal('Test'))
                    ->toSql(),
            );

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(select(star())->from(table(self::TABLE_NAME, self::SCHEMA_NAME))->toSql());

        static::assertSame('Test', $row['name']);
    }

    public function test_drop_multiple_schemas(): void
    {
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA_NAME)->toSql());
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA_NAME_RENAMED)->toSql());

        static::assertTrue($this->schemaExists(self::SCHEMA_NAME));
        static::assertTrue($this->schemaExists(self::SCHEMA_NAME_RENAMED));

        $this->pgsqlContext()->client()->execute(drop()->schema(self::SCHEMA_NAME, self::SCHEMA_NAME_RENAMED)->toSql());

        static::assertFalse($this->schemaExists(self::SCHEMA_NAME));
        static::assertFalse($this->schemaExists(self::SCHEMA_NAME_RENAMED));
    }

    public function test_drop_schema(): void
    {
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA_NAME)->toSql());

        static::assertTrue($this->schemaExists(self::SCHEMA_NAME));

        $this->pgsqlContext()->client()->execute(drop()->schema(self::SCHEMA_NAME)->toSql());

        static::assertFalse($this->schemaExists(self::SCHEMA_NAME));
    }

    public function test_drop_schema_cascade(): void
    {
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA_NAME)->toSql());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_NAME, self::SCHEMA_NAME)
                    ->column(column('id', column_type_serial()))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this->pgsqlContext()->client()->execute(drop()->schema(self::SCHEMA_NAME)->cascade()->toSql());

        static::assertFalse($this->schemaExists(self::SCHEMA_NAME));
    }

    public function test_drop_schema_if_exists(): void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(drop()->schema(self::SCHEMA_NAME)->ifExists()->toSql());
    }

    protected function schemaExists(string $name): bool
    {
        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '{$name}') AS schema_exists");

        return $row['schema_exists'] === true;
    }
}
