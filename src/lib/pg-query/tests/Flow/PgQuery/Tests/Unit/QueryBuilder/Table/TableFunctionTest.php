<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Table;

use Flow\PgQuery\Protobuf\AST\{Node, RangeFunction};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use Flow\PgQuery\QueryBuilder\Expression\{FunctionCall, Literal};
use Flow\PgQuery\QueryBuilder\Table\{AliasedTable, TableFunction};
use PHPUnit\Framework\TestCase;

final class TableFunctionTest extends TestCase
{
    public function test_as_method_returns_aliased_table() : void
    {
        $func = new FunctionCall(['generate_series'], []);
        $tableFunc = new TableFunction($func);

        $aliased = $tableFunc->as('g');

        self::assertInstanceOf(AliasedTable::class, $aliased);
        self::assertSame('g', $aliased->alias);
    }

    public function test_as_method_with_column_aliases_returns_aliased_table() : void
    {
        $func = new FunctionCall(['unnest'], []);
        $tableFunc = new TableFunction($func);

        $aliased = $tableFunc->as('u', ['value']);

        self::assertInstanceOf(AliasedTable::class, $aliased);
        self::assertSame('u', $aliased->alias);
        self::assertSame(['value'], $aliased->columnAliases);
    }

    public function test_converts_table_function_to_ast() : void
    {
        $func = new FunctionCall(['generate_series'], [Literal::int(1), Literal::int(10)]);
        $tableFunc = new TableFunction($func);

        $node = $tableFunc->toAst();

        self::assertTrue($node->hasRangeFunction());

        $rangeFunction = $node->getRangeFunction();
        self::assertInstanceOf(\Flow\PgQuery\Protobuf\AST\RangeFunction::class, $rangeFunction);
        self::assertFalse($rangeFunction->getLateral());
        self::assertFalse($rangeFunction->getOrdinality());
        self::assertFalse($rangeFunction->getIsRowsfrom());

        $functions = $rangeFunction->getFunctions();
        self::assertCount(1, $functions);

        $listNode = $functions[0];
        self::assertTrue($listNode->hasList());

        $list = $listNode->getList();
        self::assertInstanceOf(\Flow\PgQuery\Protobuf\AST\PBList::class, $list);
        $items = $list->getItems();
        self::assertCount(1, $items);
    }

    public function test_converts_table_function_with_ordinality_to_ast() : void
    {
        $func = new FunctionCall(['unnest'], []);
        $tableFunc = new TableFunction($func, true);

        $node = $tableFunc->toAst();

        self::assertTrue($node->hasRangeFunction());

        $rangeFunction = $node->getRangeFunction();
        self::assertInstanceOf(\Flow\PgQuery\Protobuf\AST\RangeFunction::class, $rangeFunction);
        self::assertTrue($rangeFunction->getOrdinality());
    }

    public function test_creates_table_function_with_function() : void
    {
        $func = new FunctionCall(['generate_series'], []);
        $tableFunc = new TableFunction($func);

        self::assertSame($func, $tableFunc->getFunction());
        self::assertFalse($tableFunc->isWithOrdinality());
    }

    public function test_creates_table_function_with_ordinality() : void
    {
        $func = new FunctionCall(['unnest'], []);
        $tableFunc = new TableFunction($func, true);

        self::assertSame($func, $tableFunc->getFunction());
        self::assertTrue($tableFunc->isWithOrdinality());
    }

    public function test_reconstructs_table_function_from_ast() : void
    {
        $func = new FunctionCall(['generate_series'], [Literal::int(1), Literal::int(5)]);
        $original = new TableFunction($func, false);

        $node = $original->toAst();
        $reconstructed = TableFunction::fromAst($node);

        self::assertSame($func->getFuncName(), $reconstructed->getFunction()->getFuncName());
        self::assertFalse($reconstructed->isWithOrdinality());
    }

    public function test_reconstructs_table_function_with_ordinality_from_ast() : void
    {
        $func = new FunctionCall(['unnest'], []);
        $original = new TableFunction($func, true);

        $node = $original->toAst();
        $reconstructed = TableFunction::fromAst($node);

        self::assertTrue($reconstructed->isWithOrdinality());
    }

    public function test_round_trip_conversion() : void
    {
        $func = new FunctionCall(['json_array_elements'], []);
        $tableFunc = new TableFunction($func, true);

        $node = $tableFunc->toAst();
        $restored = TableFunction::fromAst($node);

        self::assertSame($func->getFuncName(), $restored->getFunction()->getFuncName());
        self::assertTrue($restored->isWithOrdinality());
    }

    public function test_throws_exception_when_reconstructing_from_invalid_node_type() : void
    {
        $node = new Node();

        $this->expectException(InvalidAstException::class);

        TableFunction::fromAst($node);
    }

    public function test_throws_exception_when_reconstructing_from_range_function_without_functions() : void
    {
        $rangeFunction = new RangeFunction();

        $node = new Node();
        $node->setRangeFunction($rangeFunction);

        $this->expectException(InvalidAstException::class);

        TableFunction::fromAst($node);
    }

    public function test_with_ordinality_creates_new_instance() : void
    {
        $func = new FunctionCall(['generate_series'], []);
        $tableFunc = new TableFunction($func, false);

        $withOrdinality = $tableFunc->withOrdinality(true);

        self::assertNotSame($tableFunc, $withOrdinality);
        self::assertFalse($tableFunc->isWithOrdinality());
        self::assertTrue($withOrdinality->isWithOrdinality());
    }

    public function test_with_ordinality_default_parameter() : void
    {
        $func = new FunctionCall(['generate_series'], []);
        $tableFunc = new TableFunction($func, false);

        $withOrdinality = $tableFunc->withOrdinality();

        self::assertTrue($withOrdinality->isWithOrdinality());
    }
}
