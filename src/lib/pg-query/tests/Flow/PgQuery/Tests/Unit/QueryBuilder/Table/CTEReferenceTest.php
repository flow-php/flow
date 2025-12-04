<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Table;

use Flow\PgQuery\Protobuf\AST\{Node, RangeVar};
use Flow\PgQuery\QueryBuilder\Exception\{InvalidAstException, InvalidTableException};
use Flow\PgQuery\QueryBuilder\Table\{AliasedTable, CTEReference};
use PHPUnit\Framework\TestCase;

final class CTEReferenceTest extends TestCase
{
    public function test_as_method_returns_aliased_table() : void
    {
        $cte = new CTEReference('my_cte');

        $aliased = $cte->as('c');

        self::assertInstanceOf(AliasedTable::class, $aliased);
        self::assertSame('c', $aliased->alias);
    }

    public function test_as_method_with_column_aliases_returns_aliased_table() : void
    {
        $cte = new CTEReference('my_cte');

        $aliased = $cte->as('c', ['col1', 'col2']);

        self::assertInstanceOf(AliasedTable::class, $aliased);
        self::assertSame('c', $aliased->alias);
        self::assertSame(['col1', 'col2'], $aliased->columnAliases);
    }

    public function test_converts_cte_reference_to_ast() : void
    {
        $cte = new CTEReference('my_cte');

        $node = $cte->toAst();

        self::assertTrue($node->hasRangeVar());

        $rangeVar = $node->getRangeVar();
        self::assertInstanceOf(\Flow\PgQuery\Protobuf\AST\RangeVar::class, $rangeVar);
        self::assertSame('my_cte', $rangeVar->getRelname());
        self::assertSame('', $rangeVar->getSchemaname());
        self::assertTrue($rangeVar->getInh());
    }

    public function test_creates_cte_reference_with_name() : void
    {
        $cte = new CTEReference('my_cte');

        self::assertSame('my_cte', $cte->getCteName());
    }

    public function test_reconstructs_cte_reference_from_ast() : void
    {
        $original = new CTEReference('my_cte');

        $node = $original->toAst();
        $reconstructed = CTEReference::fromAst($node);

        self::assertSame('my_cte', $reconstructed->getCteName());
    }

    public function test_round_trip_conversion() : void
    {
        $cte = new CTEReference('recursive_cte');

        $node = $cte->toAst();
        $restored = CTEReference::fromAst($node);

        self::assertSame($cte->getCteName(), $restored->getCteName());
    }

    public function test_throws_exception_for_empty_cte_name() : void
    {
        $this->expectException(InvalidTableException::class);
        $this->expectExceptionMessage('cannot be empty');

        /** @phpstan-ignore argument.type */
        new CTEReference('');
    }

    public function test_throws_exception_when_reconstructing_from_invalid_node_type() : void
    {
        $node = new Node();

        $this->expectException(InvalidAstException::class);

        CTEReference::fromAst($node);
    }

    public function test_throws_exception_when_reconstructing_from_node_without_relname() : void
    {
        $rangeVar = new RangeVar();

        $node = new Node();
        $node->setRangeVar($rangeVar);

        $this->expectException(InvalidAstException::class);

        CTEReference::fromAst($node);
    }
}
