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
    select,
    sql_type_serial,
    sql_type_varchar,
    star,
    table
};
use Flow\PostgreSql\QueryBuilder\Schema\DataType;

use Flow\PostgreSql\QueryBuilder\Schema\Type\TypeAttribute;

final class TypeDatabaseTest extends DatabaseTestCase
{
    private const COMPOSITE_TYPE = 'flow_postgres_address_type';

    private const ENUM_TYPE = 'flow_postgres_status_enum';

    private const RANGE_TYPE = 'flow_postgres_float_range';

    private const TABLE_NAME = 'flow_postgres_type_test';

    protected function tearDown() : void
    {
        $this->dropTableIfExists(self::TABLE_NAME);
        $this->dropTypeIfExists(self::ENUM_TYPE);
        $this->dropTypeIfExists(self::COMPOSITE_TYPE);
        $this->dropTypeIfExists(self::RANGE_TYPE);

        parent::tearDown();
    }

    public function test_alter_enum_add_value() : void
    {
        $this->execute(
            create()->enumType(self::ENUM_TYPE)
                ->labels('pending', 'active')
                ->toSql()
        );

        $result = $this->execute(
            alter()->enumType(self::ENUM_TYPE)
                ->addValue('completed')
                ->toSql()
        );

        self::assertNotFalse($result);

        $values = $this->fetchAll(
            $this->execute('SELECT unnest(enum_range(NULL::' . self::ENUM_TYPE . ')) AS val')
        );

        $enumValues = \array_column($values, 'val');
        self::assertContains('completed', $enumValues);
    }

    public function test_alter_enum_add_value_after() : void
    {
        $this->execute(
            create()->enumType(self::ENUM_TYPE)
                ->labels('first', 'last')
                ->toSql()
        );

        $result = $this->execute(
            alter()->enumType(self::ENUM_TYPE)
                ->addValueAfter('middle', 'first')
                ->toSql()
        );

        self::assertNotFalse($result);

        $values = $this->fetchAll(
            $this->execute('SELECT unnest(enum_range(NULL::' . self::ENUM_TYPE . ')) AS val')
        );

        self::assertSame('first', $values[0]['val']);
        self::assertSame('middle', $values[1]['val']);
        self::assertSame('last', $values[2]['val']);
    }

    public function test_alter_enum_add_value_before() : void
    {
        $this->execute(
            create()->enumType(self::ENUM_TYPE)
                ->labels('first', 'last')
                ->toSql()
        );

        $result = $this->execute(
            alter()->enumType(self::ENUM_TYPE)
                ->addValueBefore('middle', 'last')
                ->toSql()
        );

        self::assertNotFalse($result);

        $values = $this->fetchAll(
            $this->execute('SELECT unnest(enum_range(NULL::' . self::ENUM_TYPE . ')) AS val')
        );

        self::assertSame('first', $values[0]['val']);
        self::assertSame('middle', $values[1]['val']);
        self::assertSame('last', $values[2]['val']);
    }

    public function test_alter_enum_add_value_if_not_exists() : void
    {
        $this->execute(
            create()->enumType(self::ENUM_TYPE)
                ->labels('pending', 'active')
                ->toSql()
        );

        $result = $this->execute(
            alter()->enumType(self::ENUM_TYPE)
                ->addValue('active')
                ->ifNotExists()
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_alter_enum_rename_value() : void
    {
        $this->execute(
            create()->enumType(self::ENUM_TYPE)
                ->labels('old_name', 'other')
                ->toSql()
        );

        $result = $this->execute(
            alter()->enumType(self::ENUM_TYPE)
                ->renameValue('old_name', 'new_name')
                ->toSql()
        );

        self::assertNotFalse($result);

        $values = $this->fetchAll(
            $this->execute('SELECT unnest(enum_range(NULL::' . self::ENUM_TYPE . ')) AS val')
        );

        $enumValues = \array_column($values, 'val');
        self::assertNotContains('old_name', $enumValues);
        self::assertContains('new_name', $enumValues);
    }

    public function test_create_composite_type() : void
    {
        $result = $this->execute(
            create()->compositeType(self::COMPOSITE_TYPE)
                ->attributes(
                    TypeAttribute::of('street', sql_type_varchar(100)),
                    TypeAttribute::of('city', sql_type_varchar(50)),
                    TypeAttribute::of('postal_code', sql_type_varchar(20))
                )
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->typeExists(self::COMPOSITE_TYPE));
    }

    public function test_create_composite_type_and_use_in_table() : void
    {
        $this->execute(
            create()->compositeType(self::COMPOSITE_TYPE)
                ->attributes(
                    TypeAttribute::of('street', sql_type_varchar(100)),
                    TypeAttribute::of('city', sql_type_varchar(50))
                )
                ->toSql()
        );

        $this->execute(
            create()->table(self::TABLE_NAME)
                ->column(column('id', sql_type_serial()))
                ->column(column('name', sql_type_varchar(100)))
                ->column(column('address', DataType::custom(self::COMPOSITE_TYPE)))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            'INSERT INTO ' . self::TABLE_NAME . " (name, address) VALUES ('John', ROW('123 Main St', 'NYC'))"
        );

        $row = $this->fetchOne(
            $this->execute(
                'SELECT name, (address).street, (address).city FROM ' . self::TABLE_NAME
            )
        );

        self::assertSame('John', $row['name']);
        self::assertSame('123 Main St', $row['street']);
        self::assertSame('NYC', $row['city']);
    }

    public function test_create_enum_type() : void
    {
        $result = $this->execute(
            create()->enumType(self::ENUM_TYPE)
                ->labels('pending', 'active', 'completed', 'cancelled')
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->typeExists(self::ENUM_TYPE));
    }

    public function test_create_enum_type_and_use_in_table() : void
    {
        $this->execute(
            create()->enumType(self::ENUM_TYPE)
                ->labels('pending', 'active', 'completed')
                ->toSql()
        );

        $this->execute(
            create()->table(self::TABLE_NAME)
                ->column(column('id', sql_type_serial()))
                ->column(column('status', DataType::custom(self::ENUM_TYPE))->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            insert()
                ->into(self::TABLE_NAME)
                ->columns('status')
                ->values(literal('active'))
                ->toSql()
        );

        $row = $this->fetchOne(
            $this->execute(
                select(star())->from(table(self::TABLE_NAME))->toSql()
            )
        );

        self::assertSame('active', $row['status']);
    }

    public function test_create_range_type() : void
    {
        $result = $this->execute(
            create()->rangeType(self::RANGE_TYPE)
                ->subtype('float8')
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->typeExists(self::RANGE_TYPE));
    }

    public function test_drop_type() : void
    {
        $this->execute(
            create()->enumType(self::ENUM_TYPE)
                ->labels('a', 'b')
                ->toSql()
        );

        self::assertTrue($this->typeExists(self::ENUM_TYPE));

        $result = $this->execute(
            drop()->type(self::ENUM_TYPE)->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->typeExists(self::ENUM_TYPE));
    }

    public function test_drop_type_cascade() : void
    {
        $this->execute(
            create()->enumType(self::ENUM_TYPE)
                ->labels('a', 'b')
                ->toSql()
        );

        $this->execute(
            create()->table(self::TABLE_NAME)
                ->column(column('id', sql_type_serial()))
                ->column(column('status', DataType::custom(self::ENUM_TYPE)))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $result = $this->execute(
            drop()->type(self::ENUM_TYPE)->cascade()->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->typeExists(self::ENUM_TYPE));
    }

    public function test_drop_type_if_exists() : void
    {
        $result = $this->execute(
            drop()->type(self::ENUM_TYPE)->ifExists()->toSql()
        );

        self::assertNotFalse($result);
    }

    protected function dropTypeIfExists(string $name) : void
    {
        $this->execute("DROP TYPE IF EXISTS {$name} CASCADE");
    }

    protected function typeExists(string $name) : bool
    {
        $row = $this->fetchOne(
            $this->execute("SELECT EXISTS(SELECT 1 FROM pg_type WHERE typname = '{$name}') AS type_exists")
        );

        return $row['type_exists'] === 't';
    }
}
