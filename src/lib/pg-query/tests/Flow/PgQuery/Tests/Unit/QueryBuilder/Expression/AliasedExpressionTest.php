<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Expression;

use Flow\PgQuery\Parser;
use Flow\PgQuery\Protobuf\AST\{Node, RawStmt};
use Flow\PgQuery\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PgQuery\QueryBuilder\Expression\{AliasedExpression, Column, Literal, Parameter, Star};
use Flow\PgQuery\QueryBuilder\Select\SelectBuilder;
use Flow\PgQuery\QueryBuilder\Table\Table;
use PHPUnit\Framework\TestCase;

final class AliasedExpressionTest extends TestCase
{
    public function test_aliased_column_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $aliased = AliasedExpression::create(Column::name('user_id'), 'id');

        $select = SelectBuilder::create()
            ->select($aliased)
            ->from(new Table('users'));

        $parser = new Parser();
        $ast = $select->toAst();
        $node = new Node(['select_stmt' => $ast]);
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        $deparsed = (new \Flow\PgQuery\ParsedQuery($parseResult))->deparse();

        self::assertSame('SELECT user_id AS id FROM users', $deparsed);
    }

    public function test_changes_alias() : void
    {
        $column = Column::name('users');
        $aliased = AliasedExpression::create($column, 'u');
        $newAliased = $aliased->as('user_table');

        self::assertSame('user_table', $newAliased->getAlias());
        self::assertSame($column, $newAliased->getExpression());
    }

    public function test_converts_to_ast() : void
    {
        $column = Column::name('users');
        $aliased = AliasedExpression::create($column, 'u');
        $node = $aliased->toAst();

        $resTarget = $node->getResTarget();
        self::assertNotNull($resTarget);
        self::assertSame('u', $resTarget->getName());

        $val = $resTarget->getVal();
        self::assertNotNull($val);
        self::assertNotNull($val->getColumnRef());
    }

    public function test_creates_aliased_column() : void
    {
        $column = Column::name('users');
        $aliased = AliasedExpression::create($column, 'u');

        self::assertSame($column, $aliased->getExpression());
        self::assertSame('u', $aliased->getAlias());
    }

    public function test_creates_aliased_literal() : void
    {
        $literal = Literal::int(42);
        $aliased = AliasedExpression::create($literal, 'answer');

        self::assertSame($literal, $aliased->getExpression());
        self::assertSame('answer', $aliased->getAlias());
    }

    public function test_rejects_empty_alias() : void
    {
        $this->expectException(InvalidExpressionException::class);

        AliasedExpression::create(Column::name('test'), '');
    }

    public function test_roundtrip_column_conversion() : void
    {
        $column = Column::tableColumn('users', 'name');
        $original = AliasedExpression::create($column, 'user_name');
        $node = $original->toAst();
        $reconstructed = AliasedExpression::fromAst($node);

        self::assertSame($original->getAlias(), $reconstructed->getAlias());

        $reconstructedColumn = $reconstructed->getExpression();
        self::assertInstanceOf(Column::class, $reconstructedColumn);
        self::assertEquals($column->parts(), $reconstructedColumn->parts());
    }

    public function test_roundtrip_literal_conversion() : void
    {
        $literal = Literal::string('hello');
        $original = AliasedExpression::create($literal, 'greeting');
        $node = $original->toAst();
        $reconstructed = AliasedExpression::fromAst($node);

        self::assertSame($original->getAlias(), $reconstructed->getAlias());

        $reconstructedLiteral = $reconstructed->getExpression();
        self::assertInstanceOf(Literal::class, $reconstructedLiteral);
        self::assertEquals($literal->value(), $reconstructedLiteral->value());
    }

    public function test_roundtrip_parameter_conversion() : void
    {
        $param = Parameter::positional(1);
        $original = AliasedExpression::create($param, 'p1');
        $node = $original->toAst();
        $reconstructed = AliasedExpression::fromAst($node);

        self::assertSame($original->getAlias(), $reconstructed->getAlias());

        $reconstructedParam = $reconstructed->getExpression();
        self::assertInstanceOf(Parameter::class, $reconstructedParam);
        self::assertEquals($param->number(), $reconstructedParam->number());
    }

    public function test_roundtrip_star_conversion() : void
    {
        $star = Star::fromTable('users');
        $original = AliasedExpression::create($star, 'all_users');
        $node = $original->toAst();
        $reconstructed = AliasedExpression::fromAst($node);

        self::assertSame($original->getAlias(), $reconstructed->getAlias());

        $reconstructedStar = $reconstructed->getExpression();
        self::assertInstanceOf(Star::class, $reconstructedStar);
        self::assertEquals($star->table(), $reconstructedStar->table());
    }

    public function test_with_alias() : void
    {
        $column = Column::name('users');
        $aliased = AliasedExpression::create($column, 'u');
        $newAliased = $aliased->withAlias('user_table');

        self::assertSame('user_table', $newAliased->getAlias());
        self::assertSame($column, $newAliased->getExpression());
    }

    public function test_with_expression() : void
    {
        $column = Column::name('users');
        $literal = Literal::int(42);
        $aliased = AliasedExpression::create($column, 'test');
        $newAliased = $aliased->withExpression($literal);

        self::assertSame('test', $newAliased->getAlias());
        self::assertSame($literal, $newAliased->getExpression());
    }
}
