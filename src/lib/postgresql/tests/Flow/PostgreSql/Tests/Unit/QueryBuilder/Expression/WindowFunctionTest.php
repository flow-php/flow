<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\FuncCall;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\WindowDef;
use Flow\PostgreSql\QueryBuilder\Clause\OrderBy;
use Flow\PostgreSql\QueryBuilder\Clause\SortDirection;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\WindowFunction;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

final class WindowFunctionTest extends TestCase
{
    public function test_converts_to_ast_with_full_window_definition(): void
    {
        $windowFunc = new WindowFunction(
            ['row_number'],
            [],
            [Column::name('category')],
            [new OrderBy(Column::name('created_at'), SortDirection::DESC)],
        );

        $node = $windowFunc->toAst();

        $funcCall = $node->getFuncCall();
        static::assertNotNull($funcCall);

        $over = $funcCall->getOver();
        static::assertNotNull($over);
        static::assertNotNull($over->getPartitionClause());
        static::assertCount(1, $over->getPartitionClause());
        static::assertNotNull($over->getOrderClause());
        static::assertCount(1, $over->getOrderClause());
    }

    public function test_converts_to_ast_with_minimal_window(): void
    {
        $windowFunc = new WindowFunction(['rank']);

        $node = $windowFunc->toAst();

        $funcCall = $node->getFuncCall();
        static::assertNotNull($funcCall);
        static::assertNotNull($funcCall->getOver());
    }

    public function test_creates_aliased_expression(): void
    {
        $windowFunc = new WindowFunction(['row_number']);
        $aliased = $windowFunc->as('rn');

        static::assertSame('rn', $aliased->getAlias());
        static::assertSame($windowFunc, $aliased->getExpression());
    }

    public function test_creates_window_function_with_all_components(): void
    {
        $windowFunc = new WindowFunction(
            ['rank'],
            [Column::name('score')],
            [Column::name('category')],
            [new OrderBy(Column::name('score'), SortDirection::DESC)],
        );

        static::assertSame(['rank'], $windowFunc->getFuncName());
        static::assertCount(1, $windowFunc->getArgs());
        static::assertCount(1, $windowFunc->getPartitionBy());
        static::assertCount(1, $windowFunc->getOrderBy());
    }

    public function test_creates_window_function_with_schema(): void
    {
        $windowFunc = new WindowFunction(['pg_catalog', 'row_number']);

        static::assertSame(['pg_catalog', 'row_number'], $windowFunc->getFuncName());
        static::assertSame([], $windowFunc->getArgs());
        static::assertSame([], $windowFunc->getPartitionBy());
        static::assertSame([], $windowFunc->getOrderBy());
    }

    public function test_creates_window_function_without_partition_or_order(): void
    {
        $windowFunc = new WindowFunction(['row_number']);

        static::assertSame(['row_number'], $windowFunc->getFuncName());
        static::assertSame([], $windowFunc->getArgs());
        static::assertSame([], $windowFunc->getPartitionBy());
        static::assertSame([], $windowFunc->getOrderBy());
    }

    public function test_recreates_from_ast(): void
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

        static::assertSame(['row_number'], $windowFunc->getFuncName());
        static::assertCount(1, $windowFunc->getPartitionBy());
        static::assertCount(1, $windowFunc->getOrderBy());
    }

    public function test_round_trip_conversion(): void
    {
        $windowFunc = new WindowFunction(
            ['rank'],
            [],
            [Column::name('department')],
            [new OrderBy(Column::name('salary'), SortDirection::DESC)],
        );

        $node = $windowFunc->toAst();
        $restored = WindowFunction::fromAst($node);

        static::assertSame($windowFunc->getFuncName(), $restored->getFuncName());
        static::assertCount(1, $restored->getPartitionBy());
        static::assertCount(1, $restored->getOrderBy());
    }

    public function test_throws_exception_for_empty_function_name(): void
    {
        $this->expectException(InvalidExpressionException::class);

        (new ReflectionClass(WindowFunction::class))->newInstance([]);
    }

    public function test_with_args_creates_new_instance(): void
    {
        $windowFunc = new WindowFunction(['lag']);
        $arg = Column::name('value');

        $newFunc = $windowFunc->withArgs($arg);

        static::assertNotSame($windowFunc, $newFunc);
        static::assertSame([], $windowFunc->getArgs());
        static::assertCount(1, $newFunc->getArgs());
    }

    public function test_with_order_by_creates_new_instance(): void
    {
        $windowFunc = new WindowFunction(['rank']);
        $orderBy = new OrderBy(Column::name('score'));

        $newFunc = $windowFunc->withOrderBy($orderBy);

        static::assertNotSame($windowFunc, $newFunc);
        static::assertSame([], $windowFunc->getOrderBy());
        static::assertCount(1, $newFunc->getOrderBy());
    }

    public function test_with_partition_by_creates_new_instance(): void
    {
        $windowFunc = new WindowFunction(['row_number']);
        $partition = Column::name('category');

        $newFunc = $windowFunc->withPartitionBy($partition);

        static::assertNotSame($windowFunc, $newFunc);
        static::assertSame([], $windowFunc->getPartitionBy());
        static::assertCount(1, $newFunc->getPartitionBy());
    }
}
