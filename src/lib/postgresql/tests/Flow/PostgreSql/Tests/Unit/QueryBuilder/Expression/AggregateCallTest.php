<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\FuncCall;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RawStmt;
use Flow\PostgreSql\QueryBuilder\Clause\NullsPosition;
use Flow\PostgreSql\QueryBuilder\Clause\OrderBy;
use Flow\PostgreSql\QueryBuilder\Clause\SortDirection;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\AggregateCall;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use PHPUnit\Framework\TestCase;

final class AggregateCallTest extends TestCase
{
    public function test_aggregate_count_star_deparsed_output(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $agg = new AggregateCall(['count'], [], true, false);

        $select = SelectBuilder::create()->select($agg)->from(new Table('users'));

        $parser = new Parser();
        $ast = $select->toAst();
        $node = new Node(['select_stmt' => $ast]);
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        $deparsed = (new \Flow\PostgreSql\ParsedQuery($parseResult))->deparse();

        static::assertSame('SELECT count(*) FROM users', $deparsed);
    }

    public function test_aggregate_sum_deparsed_output(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $agg = new AggregateCall(['sum'], [Column::name('amount')], false, false);

        $select = SelectBuilder::create()->select($agg)->from(new Table('orders'));

        $parser = new Parser();
        $ast = $select->toAst();
        $node = new Node(['select_stmt' => $ast]);
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        $deparsed = (new \Flow\PostgreSql\ParsedQuery($parseResult))->deparse();

        static::assertSame('SELECT sum(amount) FROM orders', $deparsed);
    }

    public function test_aggregate_with_distinct_deparsed_output(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $agg = new AggregateCall(['count'], [Column::name('user_id')], false, true);

        $select = SelectBuilder::create()->select($agg)->from(new Table('sessions'));

        $parser = new Parser();
        $ast = $select->toAst();
        $node = new Node(['select_stmt' => $ast]);
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        $deparsed = (new \Flow\PostgreSql\ParsedQuery($parseResult))->deparse();

        static::assertSame('SELECT count(DISTINCT user_id) FROM sessions', $deparsed);
    }

    public function test_converts_aggregate_with_filter_to_ast(): void
    {
        $arg = Column::name('amount');
        $filter = Literal::bool(true);
        $agg = new AggregateCall(['sum'], [$arg], false, false, [], $filter);

        $node = $agg->toAst();
        $funcCall = $node->getFuncCall();

        static::assertNotNull($funcCall);
        static::assertNotNull($funcCall->getAggFilter());
    }

    public function test_converts_aggregate_with_order_by_to_ast(): void
    {
        $arg = Column::name('value');
        $orderBy = new OrderBy(Column::name('sort_col'));
        $agg = new AggregateCall(['array_agg'], [$arg], false, false, [$orderBy]);

        $node = $agg->toAst();
        $funcCall = $node->getFuncCall();

        static::assertNotNull($funcCall);
        $orderByNodes = $funcCall->getAggOrder();
        static::assertNotNull($orderByNodes);
        static::assertCount(1, $orderByNodes);
    }

    public function test_converts_count_star_to_ast(): void
    {
        $agg = new AggregateCall(['count'], [], true);

        $node = $agg->toAst();
        $funcCall = $node->getFuncCall();

        static::assertNotNull($funcCall);
        static::assertTrue($funcCall->getAggStar());
        static::assertFalse($funcCall->getAggDistinct());
    }

    public function test_converts_distinct_aggregate_to_ast(): void
    {
        $arg = Column::name('value');
        $agg = new AggregateCall(['count'], [$arg], false, true);

        $node = $agg->toAst();
        $funcCall = $node->getFuncCall();

        static::assertNotNull($funcCall);
        static::assertTrue($funcCall->getAggDistinct());
        static::assertFalse($funcCall->getAggStar());
    }

    public function test_creates_aggregate_with_distinct(): void
    {
        $arg = Column::name('value');
        $agg = new AggregateCall(['count'], [$arg], false, true);

        static::assertTrue($agg->isDistinct());
    }

    public function test_creates_aggregate_with_filter(): void
    {
        $arg = Column::name('amount');
        $filter = Column::name('status');
        $agg = new AggregateCall(['sum'], [$arg], false, false, [], $filter);

        static::assertNotNull($agg->getFilter());
    }

    public function test_creates_aggregate_with_order_by(): void
    {
        $arg = Column::name('value');
        $orderBy = new OrderBy(Column::name('sort_col'), SortDirection::DESC);
        $agg = new AggregateCall(['array_agg'], [$arg], false, false, [$orderBy]);

        static::assertCount(1, $agg->getOrderBy());
    }

    public function test_creates_aliased_expression(): void
    {
        $agg = new AggregateCall(['count'], [], true);
        $aliased = $agg->as('total');

        static::assertSame('total', $aliased->getAlias());
        static::assertSame($agg, $aliased->getExpression());
    }

    public function test_creates_count_star_aggregate(): void
    {
        $agg = new AggregateCall(['count'], [], true);

        static::assertSame(['count'], $agg->getFuncName());
        static::assertTrue($agg->isStar());
        static::assertSame([], $agg->getArgs());
    }

    public function test_creates_sum_aggregate(): void
    {
        $arg = Column::name('amount');
        $agg = new AggregateCall(['sum'], [$arg]);

        static::assertSame(['sum'], $agg->getFuncName());
        static::assertFalse($agg->isStar());
        static::assertCount(1, $agg->getArgs());
    }

    public function test_recreates_count_star_from_ast(): void
    {
        $stringNode = new PBString();
        $stringNode->setSval('count');

        $nameNode = new Node();
        $nameNode->setString($stringNode);

        $funcCall = new FuncCall();
        $funcCall->setFuncname([$nameNode]);
        $funcCall->setAggStar(true);

        $node = new Node();
        $node->setFuncCall($funcCall);

        $agg = AggregateCall::fromAst($node);

        static::assertSame(['count'], $agg->getFuncName());
        static::assertTrue($agg->isStar());
    }

    public function test_recreates_distinct_aggregate_from_ast(): void
    {
        $stringNode = new PBString();
        $stringNode->setSval('count');

        $nameNode = new Node();
        $nameNode->setString($stringNode);

        $funcCall = new FuncCall();
        $funcCall->setFuncname([$nameNode]);
        $funcCall->setAggDistinct(true);
        $funcCall->setArgs([Literal::int(1)->toAst()]);

        $node = new Node();
        $node->setFuncCall($funcCall);

        $agg = AggregateCall::fromAst($node);

        static::assertTrue($agg->isDistinct());
    }

    public function test_round_trip_conversion_with_all_features(): void
    {
        $arg = Column::name('value');
        $orderBy = new OrderBy(Column::name('sort_col'), SortDirection::DESC, NullsPosition::LAST);
        $filter = Literal::bool(true);
        $agg = new AggregateCall(['pg_catalog', 'array_agg'], [$arg], false, true, [$orderBy], $filter);

        $node = $agg->toAst();
        $restored = AggregateCall::fromAst($node);

        static::assertSame($agg->getFuncName(), $restored->getFuncName());
        static::assertTrue($restored->isDistinct());
        static::assertCount(1, $restored->getOrderBy());
        static::assertNotNull($restored->getFilter());
    }

    public function test_throws_exception_for_empty_function_name(): void
    {
        $this->expectException(InvalidExpressionException::class);

        (new \ReflectionClass(AggregateCall::class))->newInstance([], []);
    }

    public function test_throws_exception_for_star_with_arguments(): void
    {
        $this->expectException(InvalidExpressionException::class);

        $arg = Literal::int(1);
        new AggregateCall(['count'], [$arg], true);
    }

    public function test_with_distinct_creates_new_instance(): void
    {
        $arg = Column::name('value');
        $agg = new AggregateCall(['count'], [$arg], false, false);

        $newAgg = $agg->withDistinct();

        static::assertNotSame($agg, $newAgg);
        static::assertFalse($agg->isDistinct());
        static::assertTrue($newAgg->isDistinct());
    }

    public function test_with_filter_creates_new_instance(): void
    {
        $arg = Column::name('value');
        $agg = new AggregateCall(['count'], [$arg]);

        $filter = Literal::bool(true);
        $newAgg = $agg->withFilter($filter);

        static::assertNotSame($agg, $newAgg);
        static::assertNull($agg->getFilter());
        static::assertNotNull($newAgg->getFilter());
    }

    public function test_with_order_by_creates_new_instance(): void
    {
        $arg = Column::name('value');
        $agg = new AggregateCall(['array_agg'], [$arg]);

        $orderBy = new OrderBy(Column::name('sort_col'));
        $newAgg = $agg->withOrderBy($orderBy);

        static::assertNotSame($agg, $newAgg);
        static::assertSame([], $agg->getOrderBy());
        static::assertCount(1, $newAgg->getOrderBy());
    }
}
