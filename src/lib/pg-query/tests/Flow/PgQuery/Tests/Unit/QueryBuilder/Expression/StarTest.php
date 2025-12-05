<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Expression;

use Flow\PgQuery\QueryBuilder\Expression\{AliasedExpression, Star};
use PHPUnit\Framework\TestCase;

final class StarTest extends TestCase
{
    public function test_converts_plain_star_to_ast() : void
    {
        $star = Star::all();
        $node = $star->toAst();

        $columnRef = $node->getColumnRef();
        self::assertNotNull($columnRef);

        $fields = $columnRef->getFields();
        self::assertCount(1, $fields);

        $starField = $fields[0]->getAStar();
        self::assertNotNull($starField);
    }

    public function test_converts_qualified_star_to_ast() : void
    {
        $star = Star::fromTable('users');
        $node = $star->toAst();

        $columnRef = $node->getColumnRef();
        self::assertNotNull($columnRef);

        $fields = $columnRef->getFields();
        self::assertCount(2, $fields);

        $tableField = $fields[0]->getString();
        self::assertNotNull($tableField);
        self::assertSame('users', $tableField->getSval());

        $starField = $fields[1]->getAStar();
        self::assertNotNull($starField);
    }

    public function test_creates_aliased_expression() : void
    {
        $star = Star::all();
        $aliased = $star->as('all_columns');

        self::assertInstanceOf(AliasedExpression::class, $aliased);
        self::assertSame('all_columns', $aliased->getAlias());
        self::assertSame($star, $aliased->getExpression());
    }

    public function test_creates_plain_star() : void
    {
        $star = Star::all();

        self::assertNull($star->table());
        self::assertFalse($star->isQualified());
    }

    public function test_creates_qualified_star() : void
    {
        $star = Star::fromTable('users');

        self::assertSame('users', $star->table());
        self::assertTrue($star->isQualified());
    }

    public function test_roundtrip_plain_star_conversion() : void
    {
        $original = Star::all();
        $node = $original->toAst();
        $reconstructed = Star::fromAst($node);

        self::assertEquals($original->table(), $reconstructed->table());
        self::assertFalse($reconstructed->isQualified());
    }

    public function test_roundtrip_qualified_star_conversion() : void
    {
        $original = Star::fromTable('products');
        $node = $original->toAst();
        $reconstructed = Star::fromAst($node);

        self::assertEquals($original->table(), $reconstructed->table());
        self::assertTrue($reconstructed->isQualified());
    }
}
