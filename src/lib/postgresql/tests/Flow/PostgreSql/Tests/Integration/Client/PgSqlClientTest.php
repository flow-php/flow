<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use function Flow\PostgreSql\DSL\{
    binary_expr,
    cast,
    col,
    column_type_double_precision,
    column_type_integer,
    constructor_mapper,
    create,
    delete,
    func,
    gt,
    insert,
    is_true,
    literal,
    param,
    row_expr,
    select,
    star,
    values_table
};
use Flow\PostgreSql\Client\Exception\{NoResultException, QueryException, TooManyRowsException};
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\QueryBuilder\Schema\{ColumnDefinition, ColumnType};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class PgSqlClientTest extends PostgreSqlTestCase
{
    public function test_close_disconnects() : void
    {
        $this->pgsqlContext()->client()->close();

        self::assertFalse($this->pgsqlContext()->client()->isConnected());
    }

    public function test_execute_returns_affected_rows() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->temporaryTable('test_execute')
                ->column(ColumnDefinition::create('id', ColumnType::integer()))
        );
        $this->pgsqlContext()->client()->execute(
            insert()->into('test_execute')->columns('id')
                ->values(literal(1))
                ->values(literal(2))
                ->values(literal(3))
        );

        $affected = $this->pgsqlContext()->client()->execute(
            delete()->from('test_execute')->where(gt(col('id'), literal(1)))
        );

        self::assertSame(2, $affected);
    }

    public function test_fetch_all_into_maps_to_objects() : void
    {
        $users = $this->pgsqlContext()->client()->fetchAllInto(
            constructor_mapper(TestUser::class),
            select(star())->from(
                values_table(
                    row_expr([literal(1), literal('Alice'), literal('alice@example.com')]),
                    row_expr([literal(2), literal('Bob'), literal('bob@example.com')])
                )->as('t', ['id', 'name', 'email'])
            )
        );

        self::assertCount(2, $users);
        self::assertInstanceOf(TestUser::class, $users[0]);
        self::assertSame('Alice', $users[0]->name);
        self::assertInstanceOf(TestUser::class, $users[1]);
        self::assertSame('Bob', $users[1]->name);
    }

    public function test_fetch_all_returns_all_rows() : void
    {
        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(func('generate_series', [literal(1), literal(3)])->as('num'))
        );

        self::assertCount(3, $rows);
        self::assertSame(1, $rows[0]['num']);
        self::assertSame(2, $rows[1]['num']);
        self::assertSame(3, $rows[2]['num']);
    }

    public function test_fetch_all_returns_empty_array_when_no_rows() : void
    {
        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(literal(1))->where(is_true(literal(false)))
        );

        self::assertSame([], $rows);
    }

    public function test_fetch_into_maps_to_object() : void
    {
        $user = $this->pgsqlContext()->client()->fetchInto(
            constructor_mapper(TestUser::class),
            select(literal(1)->as('id'), param(1)->as('name'), param(2)->as('email')),
            ['John Doe', 'john@example.com']
        );

        self::assertInstanceOf(TestUser::class, $user);
        self::assertSame(1, $user->id);
        self::assertSame('John Doe', $user->name);
        self::assertSame('john@example.com', $user->email);
    }

    public function test_fetch_into_returns_null_when_no_rows() : void
    {
        $user = $this->pgsqlContext()->client()->fetchInto(
            constructor_mapper(TestUser::class),
            select(literal(1)->as('id'), param(1)->as('name'), param(2)->as('email'))
                ->where(is_true(literal(false))),
            ['John', 'john@example.com']
        );

        self::assertNull($user);
    }

    public function test_fetch_one_into_maps_to_object() : void
    {
        $user = $this->pgsqlContext()->client()->fetchOneInto(
            constructor_mapper(TestUser::class),
            select(literal(1)->as('id'), param(1)->as('name'), param(2)->as('email')),
            ['Jane Doe', 'jane@example.com']
        );

        self::assertInstanceOf(TestUser::class, $user);
        self::assertSame('Jane Doe', $user->name);
    }

    public function test_fetch_one_into_returns_null_when_no_rows() : void
    {
        $user = $this->pgsqlContext()->client()->fetchOneInto(
            constructor_mapper(TestUser::class),
            select(literal(1)->as('id'), param(1)->as('name'), param(2)->as('email'))
                ->where(is_true(literal(false))),
            ['Jane Doe', 'jane@example.com']
        );

        self::assertNull($user);
    }

    public function test_fetch_one_returns_null_when_no_rows() : void
    {
        $row = $this->pgsqlContext()->client()->fetchOne(
            select(literal(1))->where(is_true(literal(false)))
        );

        self::assertNull($row);
    }

    public function test_fetch_one_returns_single_row() : void
    {
        $row = $this->pgsqlContext()->client()->fetchOne(
            select(literal(42)->as('value'))
        );

        self::assertNotNull($row);
        self::assertSame(42, $row['value']);
    }

    public function test_fetch_one_throws_when_multiple_rows() : void
    {
        $this->expectException(TooManyRowsException::class);
        $this->expectExceptionMessage('Expected at most one row, but 3 were returned');

        $this->pgsqlContext()->client()->fetchOne(
            select(func('generate_series', [literal(1), literal(3)]))
        );
    }

    public function test_fetch_returns_null_when_no_rows() : void
    {
        $row = $this->pgsqlContext()->client()->fetch(
            select(literal(1))->where(is_true(literal(false)))
        );

        self::assertNull($row);
    }

    public function test_fetch_returns_single_row() : void
    {
        $row = $this->pgsqlContext()->client()->fetch(
            select(literal(1)->as('id'), param(1)->as('name')),
            ['Alice']
        );

        self::assertIsArray($row);
        self::assertSame(1, $row['id']);
        self::assertSame('Alice', $row['name']);
    }

    public function test_fetch_scalar_bool_returns_boolean() : void
    {
        $value = $this->pgsqlContext()->client()->fetchScalarBool(
            select(literal(true))
        );

        self::assertTrue($value);

        $value = $this->pgsqlContext()->client()->fetchScalarBool(
            select(literal(false))
        );

        self::assertFalse($value);
    }

    public function test_fetch_scalar_float_returns_float() : void
    {
        $value = $this->pgsqlContext()->client()->fetchScalarFloat(
            select(cast(literal(3.14), column_type_double_precision()))
        );

        self::assertSame(3.14, $value);
    }

    public function test_fetch_scalar_int_returns_integer() : void
    {
        $value = $this->pgsqlContext()->client()->fetchScalarInt(
            select(literal(42))
        );

        self::assertSame(42, $value);
    }

    public function test_fetch_scalar_returns_null_when_no_rows() : void
    {
        $value = $this->pgsqlContext()->client()->fetchScalar(
            select(literal(1))->where(is_true(literal(false)))
        );

        self::assertNull($value);
    }

    public function test_fetch_scalar_returns_single_value() : void
    {
        $value = $this->pgsqlContext()->client()->fetchScalar(
            select(
                binary_expr(
                    cast(param(1), column_type_integer()),
                    '+',
                    cast(param(2), column_type_integer())
                )
            ),
            ['10', '32']
        );

        self::assertSame(42, $value);
    }

    public function test_fetch_scalar_string_returns_string() : void
    {
        $value = $this->pgsqlContext()->client()->fetchScalarString(
            select(literal('hello world'))
        );

        self::assertSame('hello world', $value);
    }

    public function test_fetch_single_into_maps_to_object() : void
    {
        $user = $this->pgsqlContext()->client()->fetchSingleInto(
            constructor_mapper(TestUser::class),
            select(literal(1)->as('id'), param(1)->as('name'), param(2)->as('email')),
            ['Jane Doe', 'jane@example.com']
        );

        self::assertInstanceOf(TestUser::class, $user);
        self::assertSame('Jane Doe', $user->name);
    }

    public function test_fetch_single_into_throws_when_no_rows() : void
    {
        $this->expectException(NoResultException::class);
        $this->expectExceptionMessage('Expected at least one row, but none were returned');

        $this->pgsqlContext()->client()->fetchSingleInto(
            constructor_mapper(TestUser::class),
            select(literal(1)->as('id'), param(1)->as('name'), param(2)->as('email'))
                ->where(is_true(literal(false))),
            ['Jane Doe', 'jane@example.com']
        );
    }

    public function test_fetch_single_returns_exactly_one_row() : void
    {
        $row = $this->pgsqlContext()->client()->fetchSingle(
            select(literal(42)->as('value'))
        );

        self::assertSame(42, $row['value']);
    }

    public function test_fetch_single_throws_when_multiple_rows() : void
    {
        $this->expectException(TooManyRowsException::class);
        $this->expectExceptionMessage('Expected at most one row, but 3 were returned');

        $this->pgsqlContext()->client()->fetchSingle(
            select(func('generate_series', [literal(1), literal(3)]))
        );
    }

    public function test_fetch_single_throws_when_no_rows() : void
    {
        $this->expectException(NoResultException::class);
        $this->expectExceptionMessage('Expected at least one row, but none were returned');

        $this->pgsqlContext()->client()->fetchSingle(
            select(literal(1))->where(is_true(literal(false)))
        );
    }

    public function test_is_connected() : void
    {
        self::assertTrue($this->pgsqlContext()->client()->isConnected());
    }

    public function test_last_insert_id_returns_sequence_value() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->temporaryTable('test_last_insert_id')
                ->column(ColumnDefinition::create('id', ColumnType::serial())->primaryKey())
                ->column(ColumnDefinition::create('name', ColumnType::text()))
        );
        $this->pgsqlContext()->client()->execute(
            insert()->into('test_last_insert_id')->columns('name')->values(literal('first'))
        );

        $id = $this->pgsqlContext()->client()->lastInsertId('test_last_insert_id_id_seq');

        self::assertSame(1, $id);

        $this->pgsqlContext()->client()->execute(
            insert()->into('test_last_insert_id')->columns('name')->values(literal('second'))
        );
        $id = $this->pgsqlContext()->client()->lastInsertId('test_last_insert_id_id_seq');

        self::assertSame(2, $id);
    }

    public function test_last_insert_id_throws_when_sequence_not_used() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->temporarySequence('test_unused_seq')
        );

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Query execution failed [55000]: Object not in prerequisite state. SQL: SELECT currval($1)');

        $this->pgsqlContext()->client()->lastInsertId('test_unused_seq');
    }

    public function test_typed_value_forces_type() : void
    {
        $value = new TypedValue(42, ValueType::INT4);
        $row = $this->pgsqlContext()->client()->fetchSingle(
            select(cast(param(1), column_type_integer())->as('val')),
            [$value]
        );

        self::assertSame(42, $row['val']);
    }
}

final readonly class TestUser
{
    public function __construct(
        public int $id,
        public string $name,
        public string $email,
    ) {
    }
}
