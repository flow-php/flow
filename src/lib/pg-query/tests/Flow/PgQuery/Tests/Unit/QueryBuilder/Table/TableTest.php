<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Table;

use Flow\PgQuery\QueryBuilder\Table\{AliasedTable, Table};
use PHPUnit\Framework\TestCase;

final class TableTest extends TestCase
{
    public function test_as_method_returns_aliased_table() : void
    {
        $table = new Table('users');

        $aliased = $table->as('u');

        self::assertInstanceOf(AliasedTable::class, $aliased);
    }

    public function test_as_method_with_column_aliases_returns_aliased_table() : void
    {
        $table = new Table('users');

        $aliased = $table->as('u', ['id', 'name']);

        self::assertInstanceOf(AliasedTable::class, $aliased);
        self::assertSame(['id', 'name'], $aliased->columnAliases);
    }

    public function test_converts_table_to_ast() : void
    {
        $table = new Table('users', 'public');

        $node = $table->toAst();

        self::assertTrue($node->hasRangeVar());

        $rangeVar = $node->getRangeVar();
        self::assertInstanceOf(\Flow\PgQuery\Protobuf\AST\RangeVar::class, $rangeVar);
        self::assertSame('users', $rangeVar->getRelname());
        self::assertSame('public', $rangeVar->getSchemaname());
        self::assertTrue($rangeVar->getInh());
    }

    public function test_converts_table_without_schema_to_ast() : void
    {
        $table = new Table('users');

        $node = $table->toAst();

        self::assertTrue($node->hasRangeVar());

        $rangeVar = $node->getRangeVar();
        self::assertInstanceOf(\Flow\PgQuery\Protobuf\AST\RangeVar::class, $rangeVar);
        self::assertSame('users', $rangeVar->getRelname());
        self::assertSame('', $rangeVar->getSchemaname());
        self::assertTrue($rangeVar->getInh());
    }

    public function test_creates_table_with_name_only() : void
    {
        $table = new Table('users');

        self::assertSame('users', $table->name);
        self::assertNull($table->schema);
        self::assertTrue($table->inherits);
    }

    public function test_creates_table_with_schema_and_name() : void
    {
        $table = new Table('users', 'public');

        self::assertSame('users', $table->name);
        self::assertSame('public', $table->schema);
        self::assertTrue($table->inherits);
    }

    public function test_reconstructs_table_from_ast() : void
    {
        $original = new Table('users', 'public', false);

        $node = $original->toAst();
        $reconstructed = Table::fromAst($node);

        self::assertSame('users', $reconstructed->name);
        self::assertSame('public', $reconstructed->schema);
        self::assertFalse($reconstructed->inherits);
    }

    public function test_reconstructs_table_without_schema_from_ast() : void
    {
        $original = new Table('users');

        $node = $original->toAst();
        $reconstructed = Table::fromAst($node);

        self::assertSame('users', $reconstructed->name);
        self::assertNull($reconstructed->schema);
        self::assertTrue($reconstructed->inherits);
    }

    public function test_with_name_returns_new_instance() : void
    {
        $table = new Table('users', 'public');

        $newTable = $table->withName('orders');

        self::assertNotSame($table, $newTable);
        self::assertSame('orders', $newTable->name);
        self::assertSame('public', $newTable->schema);
    }

    public function test_with_schema_returns_new_instance() : void
    {
        $table = new Table('users', 'public');

        $newTable = $table->withSchema('private');

        self::assertNotSame($table, $newTable);
        self::assertSame('users', $newTable->name);
        self::assertSame('private', $newTable->schema);
    }
}
