<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Table;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RangeFunction;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\FunctionCall;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use Flow\PostgreSql\QueryBuilder\Table\AliasedTable;
use Flow\PostgreSql\QueryBuilder\Table\TableFunction;
use PHPUnit\Framework\TestCase;

final class TableFunctionTest extends TestCase
{
    public function test_as_method_returns_aliased_table(): void
    {
        $func = new FunctionCall(['generate_series'], []);
        $tableFunc = new TableFunction($func);

        $aliased = $tableFunc->as('g');

        static::assertInstanceOf(AliasedTable::class, $aliased);
        static::assertSame('g', $aliased->alias);
    }

    public function test_as_method_with_column_aliases_returns_aliased_table(): void
    {
        $func = new FunctionCall(['unnest'], []);
        $tableFunc = new TableFunction($func);

        $aliased = $tableFunc->as('u', ['value']);

        static::assertInstanceOf(AliasedTable::class, $aliased);
        static::assertSame('u', $aliased->alias);
        static::assertSame(['value'], $aliased->columnAliases);
    }

    public function test_converts_table_function_to_ast(): void
    {
        $func = new FunctionCall(['generate_series'], [Literal::int(1), Literal::int(10)]);
        $tableFunc = new TableFunction($func);

        $node = $tableFunc->toAst();

        static::assertTrue($node->hasRangeFunction());

        $rangeFunction = $node->getRangeFunction();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeFunction::class, $rangeFunction);
        static::assertFalse($rangeFunction->getLateral());
        static::assertFalse($rangeFunction->getOrdinality());
        static::assertFalse($rangeFunction->getIsRowsfrom());

        $functions = $rangeFunction->getFunctions();
        static::assertCount(1, $functions);

        $listNode = $functions[0];
        static::assertTrue($listNode->hasList());

        $list = $listNode->getList();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\PBList::class, $list);
        $items = $list->getItems();
        static::assertCount(1, $items);
    }

    public function test_converts_table_function_with_ordinality_to_ast(): void
    {
        $func = new FunctionCall(['unnest'], []);
        $tableFunc = new TableFunction($func, true);

        $node = $tableFunc->toAst();

        static::assertTrue($node->hasRangeFunction());

        $rangeFunction = $node->getRangeFunction();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeFunction::class, $rangeFunction);
        static::assertTrue($rangeFunction->getOrdinality());
    }

    public function test_creates_table_function_with_function(): void
    {
        $func = new FunctionCall(['generate_series'], []);
        $tableFunc = new TableFunction($func);

        static::assertSame($func, $tableFunc->getFunction());
        static::assertFalse($tableFunc->isWithOrdinality());
    }

    public function test_creates_table_function_with_ordinality(): void
    {
        $func = new FunctionCall(['unnest'], []);
        $tableFunc = new TableFunction($func, true);

        static::assertSame($func, $tableFunc->getFunction());
        static::assertTrue($tableFunc->isWithOrdinality());
    }

    public function test_reconstructs_table_function_from_ast(): void
    {
        $func = new FunctionCall(['generate_series'], [Literal::int(1), Literal::int(5)]);
        $original = new TableFunction($func, false);

        $node = $original->toAst();
        $reconstructed = TableFunction::fromAst($node);

        static::assertSame($func->getFuncName(), $reconstructed->getFunction()->getFuncName());
        static::assertFalse($reconstructed->isWithOrdinality());
    }

    public function test_reconstructs_table_function_with_ordinality_from_ast(): void
    {
        $func = new FunctionCall(['unnest'], []);
        $original = new TableFunction($func, true);

        $node = $original->toAst();
        $reconstructed = TableFunction::fromAst($node);

        static::assertTrue($reconstructed->isWithOrdinality());
    }

    public function test_round_trip_conversion(): void
    {
        $func = new FunctionCall(['json_array_elements'], []);
        $tableFunc = new TableFunction($func, true);

        $node = $tableFunc->toAst();
        $restored = TableFunction::fromAst($node);

        static::assertSame($func->getFuncName(), $restored->getFunction()->getFuncName());
        static::assertTrue($restored->isWithOrdinality());
    }

    public function test_throws_exception_when_reconstructing_from_invalid_node_type(): void
    {
        $node = new Node();

        $this->expectException(InvalidAstException::class);

        TableFunction::fromAst($node);
    }

    public function test_throws_exception_when_reconstructing_from_range_function_without_functions(): void
    {
        $rangeFunction = new RangeFunction();

        $node = new Node();
        $node->setRangeFunction($rangeFunction);

        $this->expectException(InvalidAstException::class);

        TableFunction::fromAst($node);
    }

    public function test_with_ordinality_creates_new_instance(): void
    {
        $func = new FunctionCall(['generate_series'], []);
        $tableFunc = new TableFunction($func, false);

        $withOrdinality = $tableFunc->withOrdinality(true);

        static::assertNotSame($tableFunc, $withOrdinality);
        static::assertFalse($tableFunc->isWithOrdinality());
        static::assertTrue($withOrdinality->isWithOrdinality());
    }

    public function test_with_ordinality_default_parameter(): void
    {
        $func = new FunctionCall(['generate_series'], []);
        $tableFunc = new TableFunction($func, false);

        $withOrdinality = $tableFunc->withOrdinality();

        static::assertTrue($withOrdinality->isWithOrdinality());
    }
}
