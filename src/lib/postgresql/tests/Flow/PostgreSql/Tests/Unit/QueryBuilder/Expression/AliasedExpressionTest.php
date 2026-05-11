<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RawStmt;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use Flow\PostgreSql\QueryBuilder\Expression\Parameter;
use Flow\PostgreSql\QueryBuilder\Expression\Star;
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use PHPUnit\Framework\TestCase;

final class AliasedExpressionTest extends TestCase
{
    public function test_aliased_column_deparsed_output(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $aliased = AliasedExpression::create(Column::name('user_id'), 'id');

        $select = SelectBuilder::create()->select($aliased)->from(new Table('users'));

        $parser = new Parser();
        $ast = $select->toAst();
        $node = new Node(['select_stmt' => $ast]);
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        $deparsed = (new \Flow\PostgreSql\ParsedQuery($parseResult))->deparse();

        static::assertSame('SELECT user_id AS id FROM users', $deparsed);
    }

    public function test_changes_alias(): void
    {
        $column = Column::name('users');
        $aliased = AliasedExpression::create($column, 'u');
        $newAliased = $aliased->as('user_table');

        static::assertSame('user_table', $newAliased->getAlias());
        static::assertSame($column, $newAliased->getExpression());
    }

    public function test_converts_to_ast(): void
    {
        $column = Column::name('users');
        $aliased = AliasedExpression::create($column, 'u');
        $node = $aliased->toAst();

        $resTarget = $node->getResTarget();
        static::assertNotNull($resTarget);
        static::assertSame('u', $resTarget->getName());

        $val = $resTarget->getVal();
        static::assertNotNull($val);
        static::assertNotNull($val->getColumnRef());
    }

    public function test_creates_aliased_column(): void
    {
        $column = Column::name('users');
        $aliased = AliasedExpression::create($column, 'u');

        static::assertSame($column, $aliased->getExpression());
        static::assertSame('u', $aliased->getAlias());
    }

    public function test_creates_aliased_literal(): void
    {
        $literal = Literal::int(42);
        $aliased = AliasedExpression::create($literal, 'answer');

        static::assertSame($literal, $aliased->getExpression());
        static::assertSame('answer', $aliased->getAlias());
    }

    public function test_rejects_empty_alias(): void
    {
        $this->expectException(InvalidExpressionException::class);

        AliasedExpression::create(Column::name('test'), '');
    }

    public function test_roundtrip_column_conversion(): void
    {
        $column = Column::tableColumn('users', 'name');
        $original = AliasedExpression::create($column, 'user_name');
        $node = $original->toAst();
        $reconstructed = AliasedExpression::fromAst($node);

        static::assertSame($original->getAlias(), $reconstructed->getAlias());

        $reconstructedColumn = $reconstructed->getExpression();
        static::assertInstanceOf(Column::class, $reconstructedColumn);
        static::assertEquals($column->parts(), $reconstructedColumn->parts());
    }

    public function test_roundtrip_literal_conversion(): void
    {
        $literal = Literal::string('hello');
        $original = AliasedExpression::create($literal, 'greeting');
        $node = $original->toAst();
        $reconstructed = AliasedExpression::fromAst($node);

        static::assertSame($original->getAlias(), $reconstructed->getAlias());

        $reconstructedLiteral = $reconstructed->getExpression();
        static::assertInstanceOf(Literal::class, $reconstructedLiteral);
        static::assertEquals($literal->value(), $reconstructedLiteral->value());
    }

    public function test_roundtrip_parameter_conversion(): void
    {
        $param = Parameter::positional(1);
        $original = AliasedExpression::create($param, 'p1');
        $node = $original->toAst();
        $reconstructed = AliasedExpression::fromAst($node);

        static::assertSame($original->getAlias(), $reconstructed->getAlias());

        $reconstructedParam = $reconstructed->getExpression();
        static::assertInstanceOf(Parameter::class, $reconstructedParam);
        static::assertEquals($param->number(), $reconstructedParam->number());
    }

    public function test_roundtrip_star_conversion(): void
    {
        $star = Star::fromTable('users');
        $original = AliasedExpression::create($star, 'all_users');
        $node = $original->toAst();
        $reconstructed = AliasedExpression::fromAst($node);

        static::assertSame($original->getAlias(), $reconstructed->getAlias());

        $reconstructedStar = $reconstructed->getExpression();
        static::assertInstanceOf(Star::class, $reconstructedStar);
        static::assertEquals($star->table(), $reconstructedStar->table());
    }

    public function test_with_alias(): void
    {
        $column = Column::name('users');
        $aliased = AliasedExpression::create($column, 'u');
        $newAliased = $aliased->withAlias('user_table');

        static::assertSame('user_table', $newAliased->getAlias());
        static::assertSame($column, $newAliased->getExpression());
    }

    public function test_with_expression(): void
    {
        $column = Column::name('users');
        $literal = Literal::int(42);
        $aliased = AliasedExpression::create($column, 'test');
        $newAliased = $aliased->withExpression($literal);

        static::assertSame('test', $newAliased->getAlias());
        static::assertSame($literal, $newAliased->getExpression());
    }
}
