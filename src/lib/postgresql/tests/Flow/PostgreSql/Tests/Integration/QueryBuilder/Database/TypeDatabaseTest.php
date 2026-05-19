<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Schema\Type\TypeAttribute;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function array_column;
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

final class TypeDatabaseTest extends PostgreSqlTestCase
{
    private const COMPOSITE_TYPE = 'flow_postgres_address_type';

    private const ENUM_TYPE = 'flow_postgres_status_enum';

    private const RANGE_TYPE = 'flow_postgres_float_range';

    private const TABLE_NAME = 'flow_postgres_type_test';

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_NAME);
        $this->pgsqlContext()->dropTypeIfExists(self::ENUM_TYPE);
        $this->pgsqlContext()->dropTypeIfExists(self::COMPOSITE_TYPE);
        $this->pgsqlContext()->dropTypeIfExists(self::RANGE_TYPE);

        parent::tearDown();
    }

    public function test_alter_enum_add_value(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(create()->enumType(self::ENUM_TYPE)->labels('pending', 'active')->toSql());

        $this->pgsqlContext()->client()->execute(alter()->enumType(self::ENUM_TYPE)->addValue('completed')->toSql());

        $values = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll('SELECT unnest(enum_range(NULL::' . self::ENUM_TYPE . ')) AS val');

        $enumValues = array_column($values, 'val');
        static::assertContains('completed', $enumValues);
    }

    public function test_alter_enum_add_value_after(): void
    {
        $this->pgsqlContext()->client()->execute(create()->enumType(self::ENUM_TYPE)->labels('first', 'last')->toSql());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(alter()->enumType(self::ENUM_TYPE)->addValueAfter('middle', 'first')->toSql());

        $values = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll('SELECT unnest(enum_range(NULL::' . self::ENUM_TYPE . ')) AS val');

        static::assertSame('first', $values[0]['val']);
        static::assertSame('middle', $values[1]['val']);
        static::assertSame('last', $values[2]['val']);
    }

    public function test_alter_enum_add_value_before(): void
    {
        $this->pgsqlContext()->client()->execute(create()->enumType(self::ENUM_TYPE)->labels('first', 'last')->toSql());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(alter()->enumType(self::ENUM_TYPE)->addValueBefore('middle', 'last')->toSql());

        $values = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll('SELECT unnest(enum_range(NULL::' . self::ENUM_TYPE . ')) AS val');

        static::assertSame('first', $values[0]['val']);
        static::assertSame('middle', $values[1]['val']);
        static::assertSame('last', $values[2]['val']);
    }

    public function test_alter_enum_add_value_if_not_exists(): void
    {
        $this->expectNotToPerformAssertions();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(create()->enumType(self::ENUM_TYPE)->labels('pending', 'active')->toSql());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(alter()->enumType(self::ENUM_TYPE)->addValue('active')->ifNotExists()->toSql());
    }

    public function test_alter_enum_rename_value(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(create()->enumType(self::ENUM_TYPE)->labels('old_name', 'other')->toSql());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(alter()->enumType(self::ENUM_TYPE)->renameValue('old_name', 'new_name')->toSql());

        $values = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll('SELECT unnest(enum_range(NULL::' . self::ENUM_TYPE . ')) AS val');

        $enumValues = array_column($values, 'val');
        static::assertNotContains('old_name', $enumValues);
        static::assertContains('new_name', $enumValues);
    }

    public function test_create_composite_type(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->compositeType(self::COMPOSITE_TYPE)
                    ->attributes(
                        TypeAttribute::of('street', column_type_varchar(100)),
                        TypeAttribute::of('city', column_type_varchar(50)),
                        TypeAttribute::of('postal_code', column_type_varchar(20)),
                    )
                    ->toSql(),
            );

        static::assertTrue($this->typeExists(self::COMPOSITE_TYPE));
    }

    public function test_create_composite_type_and_use_in_table(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->compositeType(self::COMPOSITE_TYPE)
                    ->attributes(
                        TypeAttribute::of('street', column_type_varchar(100)),
                        TypeAttribute::of('city', column_type_varchar(50)),
                    )
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_NAME)
                    ->column(column('id', column_type_serial()))
                    ->column(column('name', column_type_varchar(100)))
                    ->column(column('address', ColumnType::custom(self::COMPOSITE_TYPE)))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                'INSERT INTO ' . self::TABLE_NAME . " (name, address) VALUES ('John', ROW('123 Main St', 'NYC'))",
            );

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle('SELECT name, (address).street, (address).city FROM ' . self::TABLE_NAME);

        static::assertSame('John', $row['name']);
        static::assertSame('123 Main St', $row['street']);
        static::assertSame('NYC', $row['city']);
    }

    public function test_create_enum_type(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()->enumType(self::ENUM_TYPE)->labels('pending', 'active', 'completed', 'cancelled')->toSql(),
            );

        static::assertTrue($this->typeExists(self::ENUM_TYPE));
    }

    public function test_create_enum_type_and_use_in_table(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(create()->enumType(self::ENUM_TYPE)->labels('pending', 'active', 'completed')->toSql());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_NAME)
                    ->column(column('id', column_type_serial()))
                    ->column(column('status', ColumnType::custom(self::ENUM_TYPE))->notNull())
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(insert()->into(self::TABLE_NAME)->columns('status')->values(literal('active'))->toSql());

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(select(star())->from(table(self::TABLE_NAME))->toSql());

        static::assertSame('active', $row['status']);
    }

    public function test_create_range_type(): void
    {
        $this->pgsqlContext()->client()->execute(create()->rangeType(self::RANGE_TYPE)->subtype('float8')->toSql());

        static::assertTrue($this->typeExists(self::RANGE_TYPE));
    }

    public function test_drop_type(): void
    {
        $this->pgsqlContext()->client()->execute(create()->enumType(self::ENUM_TYPE)->labels('a', 'b')->toSql());

        static::assertTrue($this->typeExists(self::ENUM_TYPE));

        $this->pgsqlContext()->client()->execute(drop()->type(self::ENUM_TYPE)->toSql());

        static::assertFalse($this->typeExists(self::ENUM_TYPE));
    }

    public function test_drop_type_cascade(): void
    {
        $this->pgsqlContext()->client()->execute(create()->enumType(self::ENUM_TYPE)->labels('a', 'b')->toSql());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_NAME)
                    ->column(column('id', column_type_serial()))
                    ->column(column('status', ColumnType::custom(self::ENUM_TYPE)))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this->pgsqlContext()->client()->execute(drop()->type(self::ENUM_TYPE)->cascade()->toSql());

        static::assertFalse($this->typeExists(self::ENUM_TYPE));
    }

    public function test_drop_type_if_exists(): void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(drop()->type(self::ENUM_TYPE)->ifExists()->toSql());
    }

    protected function typeExists(string $name): bool
    {
        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle("SELECT EXISTS(SELECT 1 FROM pg_type WHERE typname = '{$name}') AS type_exists");

        return $row['type_exists'] === true;
    }
}
