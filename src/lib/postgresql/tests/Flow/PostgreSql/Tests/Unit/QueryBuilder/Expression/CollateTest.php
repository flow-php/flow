<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\CollateClause;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Collate;
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\sql_parse;

final class CollateTest extends TestCase
{
    public static function collatedStatements(): Generator
    {
        yield 'ORDER BY single part' => ['SELECT a FROM t ORDER BY name COLLATE "C"'];
        yield 'ORDER BY schema qualified' => ['SELECT a FROM t ORDER BY name COLLATE pg_catalog."C" DESC'];
        yield 'target list function' => ['SELECT lower(a) COLLATE "de_DE" FROM t'];
    }

    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_as_aliases(): void
    {
        $aliased = (new Collate(col('name'), 'C'))->as('n');

        static::assertInstanceOf(AliasedExpression::class, $aliased);
        static::assertSame('SELECT name COLLATE "C" AS n', select($aliased)->toSql());
    }

    #[DataProvider('collatedStatements')]
    public function test_from_ast_round_trips(string $sql): void
    {
        $selectStmt = sql_parse($sql)->raw()->getStmts()[0]->getStmt()?->getSelectStmt();
        static::assertNotNull($selectStmt);

        static::assertSame($sql, SelectBuilder::fromAst($selectStmt)->toSql());
    }

    public function test_from_ast_throws_on_non_collate_node(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected CollateClause node, got unknown');

        Collate::fromAst(new Node());
    }

    public function test_from_ast_throws_on_non_string_collname(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Invalid value for field "collname" in CollateClause node: expected String node');

        Collate::fromAst((new Node())->setCollateClause((new CollateClause())
            ->setArg(col('name')->toAst())
            ->setCollname([
                new Node(),
            ])));
    }

    public function test_from_ast_throws_without_arg(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Missing required field "arg" in CollateClause node');

        Collate::fromAst((new Node())->setCollateClause(new CollateClause()));
    }

    public function test_from_ast_throws_without_collname(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Missing required field "collname" in CollateClause node');

        Collate::fromAst((new Node())->setCollateClause((new CollateClause())->setArg(col('name')->toAst())));
    }

    public function test_to_ast_deparses_schema_qualified_collation(): void
    {
        $collate = new Collate(col('name'), 'pg_catalog."C"');

        static::assertSame(['pg_catalog', 'C'], $collate->getCollation()->parts());
        static::assertSame('SELECT name COLLATE pg_catalog."C"', select($collate)->toSql());
    }

    public function test_to_ast_deparses_single_part_collation(): void
    {
        $collate = new Collate(col('name'), 'C');

        static::assertSame(['C'], $collate->getCollation()->parts());
        static::assertEquals(col('name'), $collate->getExpression());
        static::assertSame('SELECT name COLLATE "C"', select($collate)->toSql());
    }
}
