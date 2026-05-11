<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use PHPUnit\Framework\TestCase;

final class ColumnTest extends TestCase
{
    public function test_converts_to_ast(): void
    {
        $column = Column::tableColumn('users', 'name');
        $node = $column->toAst();

        $columnRef = $node->getColumnRef();
        static::assertNotNull($columnRef);

        $fields = $columnRef->getFields();
        static::assertCount(2, $fields);

        $firstField = $fields[0]->getString();
        static::assertNotNull($firstField);
        static::assertSame('users', $firstField->getSval());

        $secondField = $fields[1]->getString();
        static::assertNotNull($secondField);
        static::assertSame('name', $secondField->getSval());
    }

    public function test_creates_aliased_expression(): void
    {
        $column = Column::name('users');
        $aliased = $column->as('u');

        static::assertInstanceOf(AliasedExpression::class, $aliased);
        static::assertSame('u', $aliased->getAlias());
        static::assertSame($column, $aliased->getExpression());
    }

    public function test_creates_column_from_name(): void
    {
        $column = Column::name('users');

        static::assertSame(['users'], $column->parts());
        static::assertSame('users', $column->columnName());
        static::assertNull($column->tableName());
        static::assertNull($column->schemaName());
    }

    public function test_creates_column_from_parts(): void
    {
        $column = Column::fromParts(['catalog', 'schema', 'table', 'column']);

        static::assertSame(['catalog', 'schema', 'table', 'column'], $column->parts());
        static::assertSame('column', $column->columnName());
    }

    public function test_creates_column_from_schema_table_and_name(): void
    {
        $column = Column::schemaTableColumn('public', 'users', 'id');

        static::assertSame(['public', 'users', 'id'], $column->parts());
        static::assertSame('id', $column->columnName());
        static::assertSame('users', $column->tableName());
        static::assertSame('public', $column->schemaName());
    }

    public function test_creates_column_from_table_and_name(): void
    {
        $column = Column::tableColumn('users', 'id');

        static::assertSame(['users', 'id'], $column->parts());
        static::assertSame('id', $column->columnName());
        static::assertSame('users', $column->tableName());
        static::assertNull($column->schemaName());
    }

    public function test_rejects_empty_part_string(): void
    {
        $this->expectException(InvalidExpressionException::class);

        Column::fromParts(['valid', '']);
    }

    public function test_rejects_empty_parts(): void
    {
        $this->expectException(InvalidExpressionException::class);
        $this->expectExceptionMessage('Column parts cannot be empty');

        Column::fromParts([]);
    }

    public function test_roundtrip_conversion(): void
    {
        $original = Column::schemaTableColumn('public', 'users', 'id');
        $node = $original->toAst();
        $reconstructed = Column::fromAst($node);

        static::assertEquals($original->parts(), $reconstructed->parts());
    }
}
