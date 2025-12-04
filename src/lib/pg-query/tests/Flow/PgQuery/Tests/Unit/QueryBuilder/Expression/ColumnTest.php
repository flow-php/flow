<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Expression;

use Flow\PgQuery\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PgQuery\QueryBuilder\Expression\{AliasedExpression, Column};
use PHPUnit\Framework\TestCase;

final class ColumnTest extends TestCase
{
    public function test_converts_to_ast() : void
    {
        $column = Column::tableColumn('users', 'name');
        $node = $column->toAst();

        $columnRef = $node->getColumnRef();
        self::assertNotNull($columnRef);

        $fields = $columnRef->getFields();
        self::assertCount(2, $fields);

        $firstField = $fields[0]->getString();
        self::assertNotNull($firstField);
        self::assertSame('users', $firstField->getSval());

        $secondField = $fields[1]->getString();
        self::assertNotNull($secondField);
        self::assertSame('name', $secondField->getSval());
    }

    public function test_creates_aliased_expression() : void
    {
        $column = Column::name('users');
        $aliased = $column->as('u');

        self::assertInstanceOf(AliasedExpression::class, $aliased);
        self::assertSame('u', $aliased->getAlias());
        self::assertSame($column, $aliased->getExpression());
    }

    public function test_creates_column_from_name() : void
    {
        $column = Column::name('users');

        self::assertSame(['users'], $column->parts());
        self::assertSame('users', $column->columnName());
        self::assertNull($column->tableName());
        self::assertNull($column->schemaName());
    }

    public function test_creates_column_from_parts() : void
    {
        $column = Column::fromParts(['catalog', 'schema', 'table', 'column']);

        self::assertSame(['catalog', 'schema', 'table', 'column'], $column->parts());
        self::assertSame('column', $column->columnName());
    }

    public function test_creates_column_from_schema_table_and_name() : void
    {
        $column = Column::schemaTableColumn('public', 'users', 'id');

        self::assertSame(['public', 'users', 'id'], $column->parts());
        self::assertSame('id', $column->columnName());
        self::assertSame('users', $column->tableName());
        self::assertSame('public', $column->schemaName());
    }

    public function test_creates_column_from_table_and_name() : void
    {
        $column = Column::tableColumn('users', 'id');

        self::assertSame(['users', 'id'], $column->parts());
        self::assertSame('id', $column->columnName());
        self::assertSame('users', $column->tableName());
        self::assertNull($column->schemaName());
    }

    public function test_rejects_empty_part_string() : void
    {
        $this->expectException(InvalidExpressionException::class);

        Column::fromParts(['valid', '']);
    }

    public function test_rejects_empty_parts() : void
    {
        $this->expectException(InvalidExpressionException::class);
        $this->expectExceptionMessage('Column parts cannot be empty');

        Column::fromParts([]);
    }

    public function test_roundtrip_conversion() : void
    {
        $original = Column::schemaTableColumn('public', 'users', 'id');
        $node = $original->toAst();
        $reconstructed = Column::fromAst($node);

        self::assertEquals($original->parts(), $reconstructed->parts());
    }
}
