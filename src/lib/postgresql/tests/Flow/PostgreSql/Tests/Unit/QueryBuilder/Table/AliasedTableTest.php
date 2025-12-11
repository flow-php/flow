<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Table;

use Flow\PostgreSql\QueryBuilder\Table\{AliasedTable, Table};
use PHPUnit\Framework\TestCase;

final class AliasedTableTest extends TestCase
{
    public function test_as_method_returns_new_aliased_table() : void
    {
        $table = new Table('users');
        $aliased = new AliasedTable($table, 'u');

        $newAliased = $aliased->as('usr');

        self::assertNotSame($aliased, $newAliased);
        self::assertInstanceOf(AliasedTable::class, $newAliased);
        self::assertSame('usr', $newAliased->alias);
    }

    public function test_converts_aliased_table_to_ast() : void
    {
        $table = new Table('users', 'public');
        $aliased = new AliasedTable($table, 'u');

        $node = $aliased->toAst();

        self::assertTrue($node->hasRangeVar());

        $rangeVar = $node->getRangeVar();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeVar::class, $rangeVar);
        self::assertSame('users', $rangeVar->getRelname());
        self::assertSame('public', $rangeVar->getSchemaname());

        $alias = $rangeVar->getAlias();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Alias::class, $alias);
        self::assertSame('u', $alias->getAliasname());
        self::assertCount(0, $alias->getColnames());
    }

    public function test_converts_aliased_table_with_column_aliases_to_ast() : void
    {
        $table = new Table('users');
        $aliased = new AliasedTable($table, 'u', ['id', 'name']);

        $node = $aliased->toAst();

        self::assertTrue($node->hasRangeVar());

        $rangeVar = $node->getRangeVar();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeVar::class, $rangeVar);
        $alias = $rangeVar->getAlias();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Alias::class, $alias);

        self::assertSame('u', $alias->getAliasname());

        $colnames = $alias->getColnames();
        self::assertCount(2, $colnames);

        $col1 = $colnames[0]->getString();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\PBString::class, $col1);
        self::assertSame('id', $col1->getSval());

        $col2 = $colnames[1]->getString();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\PBString::class, $col2);
        self::assertSame('name', $col2->getSval());
    }

    public function test_creates_aliased_table() : void
    {
        $table = new Table('users');
        $aliased = new AliasedTable($table, 'u');

        self::assertSame($table, $aliased->table);
        self::assertSame('u', $aliased->alias);
        self::assertNull($aliased->columnAliases);
    }

    public function test_creates_aliased_table_with_column_aliases() : void
    {
        $table = new Table('users');
        $aliased = new AliasedTable($table, 'u', ['id', 'name']);

        self::assertSame($table, $aliased->table);
        self::assertSame('u', $aliased->alias);
        self::assertSame(['id', 'name'], $aliased->columnAliases);
    }

    public function test_reconstructs_aliased_table_from_ast() : void
    {
        $table = new Table('users', 'public');
        $original = new AliasedTable($table, 'u');

        $node = $original->toAst();
        $reconstructed = AliasedTable::fromAst($node);

        self::assertInstanceOf(AliasedTable::class, $reconstructed);
        self::assertSame('u', $reconstructed->alias);
        self::assertInstanceOf(Table::class, $reconstructed->table);
        self::assertSame('users', $reconstructed->table->name);
        self::assertSame('public', $reconstructed->table->schema);
        self::assertNull($reconstructed->columnAliases);
    }

    public function test_reconstructs_aliased_table_with_column_aliases_from_ast() : void
    {
        $table = new Table('users');
        $original = new AliasedTable($table, 'u', ['id', 'name']);

        $node = $original->toAst();
        $reconstructed = AliasedTable::fromAst($node);

        self::assertInstanceOf(AliasedTable::class, $reconstructed);
        self::assertSame('u', $reconstructed->alias);
        self::assertSame(['id', 'name'], $reconstructed->columnAliases);
    }
}
