<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Table;

use Flow\PostgreSql\QueryBuilder\Table\AliasedTable;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use PHPUnit\Framework\TestCase;

final class AliasedTableTest extends TestCase
{
    public function test_as_method_returns_new_aliased_table(): void
    {
        $table = new Table('users');
        $aliased = new AliasedTable($table, 'u');

        $newAliased = $aliased->as('usr');

        static::assertNotSame($aliased, $newAliased);
        static::assertInstanceOf(AliasedTable::class, $newAliased);
        static::assertSame('usr', $newAliased->alias);
    }

    public function test_converts_aliased_table_to_ast(): void
    {
        $table = new Table('users', 'public');
        $aliased = new AliasedTable($table, 'u');

        $node = $aliased->toAst();

        static::assertTrue($node->hasRangeVar());

        $rangeVar = $node->getRangeVar();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeVar::class, $rangeVar);
        static::assertSame('users', $rangeVar->getRelname());
        static::assertSame('public', $rangeVar->getSchemaname());

        $alias = $rangeVar->getAlias();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Alias::class, $alias);
        static::assertSame('u', $alias->getAliasname());
        static::assertCount(0, $alias->getColnames());
    }

    public function test_converts_aliased_table_with_column_aliases_to_ast(): void
    {
        $table = new Table('users');
        $aliased = new AliasedTable($table, 'u', ['id', 'name']);

        $node = $aliased->toAst();

        static::assertTrue($node->hasRangeVar());

        $rangeVar = $node->getRangeVar();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeVar::class, $rangeVar);
        $alias = $rangeVar->getAlias();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Alias::class, $alias);

        static::assertSame('u', $alias->getAliasname());

        $colnames = $alias->getColnames();
        static::assertCount(2, $colnames);

        $col1 = $colnames[0]->getString();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\PBString::class, $col1);
        static::assertSame('id', $col1->getSval());

        $col2 = $colnames[1]->getString();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\PBString::class, $col2);
        static::assertSame('name', $col2->getSval());
    }

    public function test_creates_aliased_table(): void
    {
        $table = new Table('users');
        $aliased = new AliasedTable($table, 'u');

        static::assertSame($table, $aliased->table);
        static::assertSame('u', $aliased->alias);
        static::assertNull($aliased->columnAliases);
    }

    public function test_creates_aliased_table_with_column_aliases(): void
    {
        $table = new Table('users');
        $aliased = new AliasedTable($table, 'u', ['id', 'name']);

        static::assertSame($table, $aliased->table);
        static::assertSame('u', $aliased->alias);
        static::assertSame(['id', 'name'], $aliased->columnAliases);
    }

    public function test_reconstructs_aliased_table_from_ast(): void
    {
        $table = new Table('users', 'public');
        $original = new AliasedTable($table, 'u');

        $node = $original->toAst();
        $reconstructed = AliasedTable::fromAst($node);

        static::assertInstanceOf(AliasedTable::class, $reconstructed);
        static::assertSame('u', $reconstructed->alias);
        static::assertInstanceOf(Table::class, $reconstructed->table);
        static::assertSame('users', $reconstructed->table->name);
        static::assertSame('public', $reconstructed->table->schema);
        static::assertNull($reconstructed->columnAliases);
    }

    public function test_reconstructs_aliased_table_with_column_aliases_from_ast(): void
    {
        $table = new Table('users');
        $original = new AliasedTable($table, 'u', ['id', 'name']);

        $node = $original->toAst();
        $reconstructed = AliasedTable::fromAst($node);

        static::assertInstanceOf(AliasedTable::class, $reconstructed);
        static::assertSame('u', $reconstructed->alias);
        static::assertSame(['id', 'name'], $reconstructed->columnAliases);
    }
}
