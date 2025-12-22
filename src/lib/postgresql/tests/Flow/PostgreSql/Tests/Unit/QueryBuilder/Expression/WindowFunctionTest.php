<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\{FuncCall, Node, PBString, WindowDef};
use Flow\PostgreSql\QueryBuilder\Clause\{OrderBy, SortDirection};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\{Column, WindowFunction};
use PHPUnit\Framework\TestCase;

final class WindowFunctionTest extends TestCase
{
    public function test_converts_to_ast_with_full_window_definition() : void
    {
        $windowFunc = new WindowFunction(
            ['row_number'],
            [],
            [Column::name('category')],
            [new OrderBy(Column::name('created_at'), SortDirection::DESC)]
        );

        $node = $windowFunc->toAst();

        self::assertNotNull($node->getFuncCall());

        $funcCall = $node->getFuncCall();
        self::assertNotNull($funcCall->getOver());

        $over = $funcCall->getOver();
        self::assertNotNull($over->getPartitionClause());
        self::assertCount(1, $over->getPartitionClause());
        self::assertNotNull($over->getOrderClause());
        self::assertCount(1, $over->getOrderClause());
    }

    public function test_converts_to_ast_with_minimal_window() : void
    {
        $windowFunc = new WindowFunction(['rank']);

        $node = $windowFunc->toAst();

        self::assertNotNull($node->getFuncCall());

        $funcCall = $node->getFuncCall();
        self::assertNotNull($funcCall->getOver());
    }

    public function test_creates_aliased_expression() : void
    {
        $windowFunc = new WindowFunction(['row_number']);
        $aliased = $windowFunc->as('rn');

        self::assertSame('rn', $aliased->getAlias());
        self::assertSame($windowFunc, $aliased->getExpression());
    }

    public function test_creates_window_function_with_all_components() : void
    {
        $windowFunc = new WindowFunction(
            ['rank'],
            [Column::name('score')],
            [Column::name('category')],
            [new OrderBy(Column::name('score'), SortDirection::DESC)]
        );

        self::assertSame(['rank'], $windowFunc->getFuncName());
        self::assertCount(1, $windowFunc->getArgs());
        self::assertCount(1, $windowFunc->getPartitionBy());
        self::assertCount(1, $windowFunc->getOrderBy());
    }

    public function test_creates_window_function_with_schema() : void
    {
        $windowFunc = new WindowFunction(['pg_catalog', 'row_number']);

        self::assertSame(['pg_catalog', 'row_number'], $windowFunc->getFuncName());
        self::assertSame([], $windowFunc->getArgs());
        self::assertSame([], $windowFunc->getPartitionBy());
        self::assertSame([], $windowFunc->getOrderBy());
    }

    public function test_creates_window_function_without_partition_or_order() : void
    {
        $windowFunc = new WindowFunction(['row_number']);

        self::assertSame(['row_number'], $windowFunc->getFuncName());
        self::assertSame([], $windowFunc->getArgs());
        self::assertSame([], $windowFunc->getPartitionBy());
        self::assertSame([], $windowFunc->getOrderBy());
    }

    public function test_recreates_from_ast() : void
    {
        $funcNameNode = new Node();
        $funcNameNode->setString((new PBString())->setSval('row_number'));

        $partitionNode = Column::name('category')->toAst();
        $orderByNode = (new OrderBy(Column::name('created_at')))->toNode();

        $windowDef = new WindowDef();
        $windowDef->setPartitionClause([$partitionNode]);
        $windowDef->setOrderClause([$orderByNode]);

        $funcCall = new FuncCall();
        $funcCall->setFuncname([$funcNameNode]);
        $funcCall->setOver($windowDef);

        $node = new Node();
        $node->setFuncCall($funcCall);

        $windowFunc = WindowFunction::fromAst($node);

        self::assertSame(['row_number'], $windowFunc->getFuncName());
        self::assertCount(1, $windowFunc->getPartitionBy());
        self::assertCount(1, $windowFunc->getOrderBy());
    }

    public function test_round_trip_conversion() : void
    {
        $windowFunc = new WindowFunction(
            ['rank'],
            [],
            [Column::name('department')],
            [new OrderBy(Column::name('salary'), SortDirection::DESC)]
        );

        $node = $windowFunc->toAst();
        $restored = WindowFunction::fromAst($node);

        self::assertSame($windowFunc->getFuncName(), $restored->getFuncName());
        self::assertCount(1, $restored->getPartitionBy());
        self::assertCount(1, $restored->getOrderBy());
    }

    public function test_throws_exception_for_empty_function_name() : void
    {
        $this->expectException(InvalidExpressionException::class);

        /** @phpstan-ignore argument.type (intentionally testing exception) */
        new WindowFunction([]);
    }

    public function test_with_args_creates_new_instance() : void
    {
        $windowFunc = new WindowFunction(['lag']);
        $arg = Column::name('value');

        $newFunc = $windowFunc->withArgs($arg);

        self::assertNotSame($windowFunc, $newFunc);
        self::assertSame([], $windowFunc->getArgs());
        self::assertCount(1, $newFunc->getArgs());
    }

    public function test_with_order_by_creates_new_instance() : void
    {
        $windowFunc = new WindowFunction(['rank']);
        $orderBy = new OrderBy(Column::name('score'));

        $newFunc = $windowFunc->withOrderBy($orderBy);

        self::assertNotSame($windowFunc, $newFunc);
        self::assertSame([], $windowFunc->getOrderBy());
        self::assertCount(1, $newFunc->getOrderBy());
    }

    public function test_with_partition_by_creates_new_instance() : void
    {
        $windowFunc = new WindowFunction(['row_number']);
        $partition = Column::name('category');

        $newFunc = $windowFunc->withPartitionBy($partition);

        self::assertNotSame($windowFunc, $newFunc);
        self::assertSame([], $windowFunc->getPartitionBy());
        self::assertCount(1, $newFunc->getPartitionBy());
    }
}
