<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Table;

use Flow\PgQuery\Protobuf\AST\{Node, RangeFunction, RangeSubselect, SelectStmt};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use Flow\PgQuery\QueryBuilder\Expression\FunctionCall;
use Flow\PgQuery\QueryBuilder\Table\{AliasedTable, Lateral, SubqueryReference, TableFunction};
use PHPUnit\Framework\TestCase;

final class LateralTest extends TestCase
{
    public function test_as_method_returns_aliased_table() : void
    {
        $subquery = $this->createSubqueryNode();
        $lateral = new Lateral(new SubqueryReference($subquery));

        $aliased = $lateral->as('l');

        self::assertInstanceOf(AliasedTable::class, $aliased);
        self::assertSame('l', $aliased->alias);
    }

    public function test_as_method_with_column_aliases_returns_aliased_table() : void
    {
        $subquery = $this->createSubqueryNode();
        $lateral = new Lateral(new SubqueryReference($subquery));

        $aliased = $lateral->as('l', ['col1', 'col2']);

        self::assertInstanceOf(AliasedTable::class, $aliased);
        self::assertSame('l', $aliased->alias);
        self::assertSame(['col1', 'col2'], $aliased->columnAliases);
    }

    public function test_converts_lateral_function_to_ast() : void
    {
        $func = new FunctionCall(['unnest'], []);
        $tableFunc = new TableFunction($func);
        $lateral = new Lateral($tableFunc);

        $node = $lateral->toAst();

        self::assertTrue($node->hasRangeFunction());

        $rangeFunction = $node->getRangeFunction();
        self::assertInstanceOf(\Flow\PgQuery\Protobuf\AST\RangeFunction::class, $rangeFunction);
        self::assertTrue($rangeFunction->getLateral());
        self::assertFalse($rangeFunction->getOrdinality());
    }

    public function test_converts_lateral_subquery_to_ast() : void
    {
        $subquery = $this->createSubqueryNode();
        $lateral = new Lateral(new SubqueryReference($subquery));

        $node = $lateral->toAst();

        self::assertTrue($node->hasRangeSubselect());

        $rangeSubselect = $node->getRangeSubselect();
        self::assertInstanceOf(\Flow\PgQuery\Protobuf\AST\RangeSubselect::class, $rangeSubselect);
        self::assertTrue($rangeSubselect->getLateral());
        $subqueryNode = $rangeSubselect->getSubquery();
        self::assertInstanceOf(\Flow\PgQuery\Protobuf\AST\Node::class, $subqueryNode);
    }

    public function test_creates_lateral_with_function() : void
    {
        $func = new FunctionCall(['generate_series'], []);
        $tableFunc = new TableFunction($func);
        $lateral = new Lateral($tableFunc);

        self::assertInstanceOf(TableFunction::class, $lateral->getReference());
    }

    public function test_creates_lateral_with_subquery() : void
    {
        $subquery = $this->createSubqueryNode();
        $lateral = new Lateral(new SubqueryReference($subquery));

        self::assertInstanceOf(SubqueryReference::class, $lateral->getReference());
    }

    public function test_reconstructs_lateral_function_from_ast() : void
    {
        $func = new FunctionCall(['unnest'], []);
        $tableFunc = new TableFunction($func);
        $original = new Lateral($tableFunc);

        $node = $original->toAst();
        $reconstructed = Lateral::fromAst($node);

        self::assertInstanceOf(TableFunction::class, $reconstructed->getReference());
    }

    public function test_reconstructs_lateral_subquery_from_ast() : void
    {
        $subquery = $this->createSubqueryNode();
        $original = new Lateral(new SubqueryReference($subquery));

        $node = $original->toAst();
        $reconstructed = Lateral::fromAst($node);

        self::assertInstanceOf(SubqueryReference::class, $reconstructed->getReference());
    }

    public function test_round_trip_conversion_with_function() : void
    {
        $func = new FunctionCall(['generate_series'], []);
        $tableFunc = new TableFunction($func);
        $lateral = new Lateral($tableFunc);

        $node = $lateral->toAst();
        $restored = Lateral::fromAst($node);

        self::assertTrue($node->hasRangeFunction());
        self::assertInstanceOf(TableFunction::class, $restored->getReference());
    }

    public function test_round_trip_conversion_with_subquery() : void
    {
        $subquery = $this->createSubqueryNode();
        $lateral = new Lateral(new SubqueryReference($subquery));

        $node = $lateral->toAst();
        $restored = Lateral::fromAst($node);

        self::assertTrue($node->hasRangeSubselect());
        self::assertInstanceOf(SubqueryReference::class, $restored->getReference());
    }

    public function test_throws_exception_when_reconstructing_from_invalid_node_type() : void
    {
        $node = new Node();

        $this->expectException(InvalidAstException::class);

        Lateral::fromAst($node);
    }

    public function test_throws_exception_when_reconstructing_from_range_function_without_lateral() : void
    {
        $rangeFunction = new RangeFunction();
        $rangeFunction->setLateral(false);

        $node = new Node();
        $node->setRangeFunction($rangeFunction);

        $this->expectException(InvalidAstException::class);

        Lateral::fromAst($node);
    }

    public function test_throws_exception_when_reconstructing_from_range_subselect_without_lateral() : void
    {
        $rangeSubselect = new RangeSubselect();
        $rangeSubselect->setLateral(false);
        $rangeSubselect->setSubquery($this->createSubqueryNode());

        $node = new Node();
        $node->setRangeSubselect($rangeSubselect);

        $this->expectException(InvalidAstException::class);

        Lateral::fromAst($node);
    }

    private function createSubqueryNode() : Node
    {
        $selectStmt = new SelectStmt();

        $node = new Node();
        $node->setSelectStmt($selectStmt);

        return $node;
    }
}
