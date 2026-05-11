<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Table;

use Flow\PostgreSql\QueryBuilder\Table\AliasedTable;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use PHPUnit\Framework\TestCase;

final class TableTest extends TestCase
{
    public function test_as_method_returns_aliased_table(): void
    {
        $table = new Table('users');

        $aliased = $table->as('u');

        static::assertInstanceOf(AliasedTable::class, $aliased);
    }

    public function test_as_method_with_column_aliases_returns_aliased_table(): void
    {
        $table = new Table('users');

        $aliased = $table->as('u', ['id', 'name']);

        static::assertInstanceOf(AliasedTable::class, $aliased);
        static::assertSame(['id', 'name'], $aliased->columnAliases);
    }

    public function test_converts_table_to_ast(): void
    {
        $table = new Table('users', 'public');

        $node = $table->toAst();

        static::assertTrue($node->hasRangeVar());

        $rangeVar = $node->getRangeVar();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeVar::class, $rangeVar);
        static::assertSame('users', $rangeVar->getRelname());
        static::assertSame('public', $rangeVar->getSchemaname());
        static::assertTrue($rangeVar->getInh());
    }

    public function test_converts_table_without_schema_to_ast(): void
    {
        $table = new Table('users');

        $node = $table->toAst();

        static::assertTrue($node->hasRangeVar());

        $rangeVar = $node->getRangeVar();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeVar::class, $rangeVar);
        static::assertSame('users', $rangeVar->getRelname());
        static::assertSame('', $rangeVar->getSchemaname());
        static::assertTrue($rangeVar->getInh());
    }

    public function test_creates_table_with_name_only(): void
    {
        $table = new Table('users');

        static::assertSame('users', $table->name);
        static::assertNull($table->schema);
        static::assertTrue($table->inherits);
    }

    public function test_creates_table_with_schema_and_name(): void
    {
        $table = new Table('users', 'public');

        static::assertSame('users', $table->name);
        static::assertSame('public', $table->schema);
        static::assertTrue($table->inherits);
    }

    public function test_reconstructs_table_from_ast(): void
    {
        $original = new Table('users', 'public', false);

        $node = $original->toAst();
        $reconstructed = Table::fromAst($node);

        static::assertSame('users', $reconstructed->name);
        static::assertSame('public', $reconstructed->schema);
        static::assertFalse($reconstructed->inherits);
    }

    public function test_reconstructs_table_without_schema_from_ast(): void
    {
        $original = new Table('users');

        $node = $original->toAst();
        $reconstructed = Table::fromAst($node);

        static::assertSame('users', $reconstructed->name);
        static::assertNull($reconstructed->schema);
        static::assertTrue($reconstructed->inherits);
    }

    public function test_with_name_returns_new_instance(): void
    {
        $table = new Table('users', 'public');

        $newTable = $table->withName('orders');

        static::assertNotSame($table, $newTable);
        static::assertSame('orders', $newTable->name);
        static::assertSame('public', $newTable->schema);
    }

    public function test_with_schema_returns_new_instance(): void
    {
        $table = new Table('users', 'public');

        $newTable = $table->withSchema('private');

        static::assertNotSame($table, $newTable);
        static::assertSame('users', $newTable->name);
        static::assertSame('private', $newTable->schema);
    }
}
