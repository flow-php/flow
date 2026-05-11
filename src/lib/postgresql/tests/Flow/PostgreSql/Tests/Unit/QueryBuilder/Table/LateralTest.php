<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Table;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RangeFunction;
use Flow\PostgreSql\Protobuf\AST\RangeSubselect;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\FunctionCall;
use Flow\PostgreSql\QueryBuilder\Table\AliasedTable;
use Flow\PostgreSql\QueryBuilder\Table\Lateral;
use Flow\PostgreSql\QueryBuilder\Table\SubqueryReference;
use Flow\PostgreSql\QueryBuilder\Table\TableFunction;
use PHPUnit\Framework\TestCase;

final class LateralTest extends TestCase
{
    public function test_as_method_returns_aliased_table(): void
    {
        $subquery = $this->createSubqueryNode();
        $lateral = new Lateral(new SubqueryReference($subquery));

        $aliased = $lateral->as('l');

        static::assertInstanceOf(AliasedTable::class, $aliased);
        static::assertSame('l', $aliased->alias);
    }

    public function test_as_method_with_column_aliases_returns_aliased_table(): void
    {
        $subquery = $this->createSubqueryNode();
        $lateral = new Lateral(new SubqueryReference($subquery));

        $aliased = $lateral->as('l', ['col1', 'col2']);

        static::assertInstanceOf(AliasedTable::class, $aliased);
        static::assertSame('l', $aliased->alias);
        static::assertSame(['col1', 'col2'], $aliased->columnAliases);
    }

    public function test_converts_lateral_function_to_ast(): void
    {
        $func = new FunctionCall(['unnest'], []);
        $tableFunc = new TableFunction($func);
        $lateral = new Lateral($tableFunc);

        $node = $lateral->toAst();

        static::assertTrue($node->hasRangeFunction());

        $rangeFunction = $node->getRangeFunction();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeFunction::class, $rangeFunction);
        static::assertTrue($rangeFunction->getLateral());
        static::assertFalse($rangeFunction->getOrdinality());
    }

    public function test_converts_lateral_subquery_to_ast(): void
    {
        $subquery = $this->createSubqueryNode();
        $lateral = new Lateral(new SubqueryReference($subquery));

        $node = $lateral->toAst();

        static::assertTrue($node->hasRangeSubselect());

        $rangeSubselect = $node->getRangeSubselect();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeSubselect::class, $rangeSubselect);
        static::assertTrue($rangeSubselect->getLateral());
        $subqueryNode = $rangeSubselect->getSubquery();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Node::class, $subqueryNode);
    }

    public function test_creates_lateral_with_function(): void
    {
        $func = new FunctionCall(['generate_series'], []);
        $tableFunc = new TableFunction($func);
        $lateral = new Lateral($tableFunc);

        static::assertInstanceOf(TableFunction::class, $lateral->getReference());
    }

    public function test_creates_lateral_with_subquery(): void
    {
        $subquery = $this->createSubqueryNode();
        $lateral = new Lateral(new SubqueryReference($subquery));

        static::assertInstanceOf(SubqueryReference::class, $lateral->getReference());
    }

    public function test_reconstructs_lateral_function_from_ast(): void
    {
        $func = new FunctionCall(['unnest'], []);
        $tableFunc = new TableFunction($func);
        $original = new Lateral($tableFunc);

        $node = $original->toAst();
        $reconstructed = Lateral::fromAst($node);

        static::assertInstanceOf(TableFunction::class, $reconstructed->getReference());
    }

    public function test_reconstructs_lateral_subquery_from_ast(): void
    {
        $subquery = $this->createSubqueryNode();
        $original = new Lateral(new SubqueryReference($subquery));

        $node = $original->toAst();
        $reconstructed = Lateral::fromAst($node);

        static::assertInstanceOf(SubqueryReference::class, $reconstructed->getReference());
    }

    public function test_round_trip_conversion_with_function(): void
    {
        $func = new FunctionCall(['generate_series'], []);
        $tableFunc = new TableFunction($func);
        $lateral = new Lateral($tableFunc);

        $node = $lateral->toAst();
        $restored = Lateral::fromAst($node);

        static::assertTrue($node->hasRangeFunction());
        static::assertInstanceOf(TableFunction::class, $restored->getReference());
    }

    public function test_round_trip_conversion_with_subquery(): void
    {
        $subquery = $this->createSubqueryNode();
        $lateral = new Lateral(new SubqueryReference($subquery));

        $node = $lateral->toAst();
        $restored = Lateral::fromAst($node);

        static::assertTrue($node->hasRangeSubselect());
        static::assertInstanceOf(SubqueryReference::class, $restored->getReference());
    }

    public function test_throws_exception_when_reconstructing_from_invalid_node_type(): void
    {
        $node = new Node();

        $this->expectException(InvalidAstException::class);

        Lateral::fromAst($node);
    }

    public function test_throws_exception_when_reconstructing_from_range_function_without_lateral(): void
    {
        $rangeFunction = new RangeFunction();
        $rangeFunction->setLateral(false);

        $node = new Node();
        $node->setRangeFunction($rangeFunction);

        $this->expectException(InvalidAstException::class);

        Lateral::fromAst($node);
    }

    public function test_throws_exception_when_reconstructing_from_range_subselect_without_lateral(): void
    {
        $rangeSubselect = new RangeSubselect();
        $rangeSubselect->setLateral(false);
        $rangeSubselect->setSubquery($this->createSubqueryNode());

        $node = new Node();
        $node->setRangeSubselect($rangeSubselect);

        $this->expectException(InvalidAstException::class);

        Lateral::fromAst($node);
    }

    private function createSubqueryNode(): Node
    {
        $selectStmt = new SelectStmt();

        $node = new Node();
        $node->setSelectStmt($selectStmt);

        return $node;
    }
}
