<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Star;
use PHPUnit\Framework\TestCase;

final class StarTest extends TestCase
{
    public function test_converts_plain_star_to_ast(): void
    {
        $star = Star::all();
        $node = $star->toAst();

        $columnRef = $node->getColumnRef();
        static::assertNotNull($columnRef);

        $fields = $columnRef->getFields();
        static::assertCount(1, $fields);

        $starField = $fields[0]->getAStar();
        static::assertNotNull($starField);
    }

    public function test_converts_qualified_star_to_ast(): void
    {
        $star = Star::fromTable('users');
        $node = $star->toAst();

        $columnRef = $node->getColumnRef();
        static::assertNotNull($columnRef);

        $fields = $columnRef->getFields();
        static::assertCount(2, $fields);

        $tableField = $fields[0]->getString();
        static::assertNotNull($tableField);
        static::assertSame('users', $tableField->getSval());

        $starField = $fields[1]->getAStar();
        static::assertNotNull($starField);
    }

    public function test_creates_aliased_expression(): void
    {
        $star = Star::all();
        $aliased = $star->as('all_columns');

        static::assertInstanceOf(AliasedExpression::class, $aliased);
        static::assertSame('all_columns', $aliased->getAlias());
        static::assertSame($star, $aliased->getExpression());
    }

    public function test_creates_plain_star(): void
    {
        $star = Star::all();

        static::assertNull($star->table());
        static::assertFalse($star->isQualified());
    }

    public function test_creates_qualified_star(): void
    {
        $star = Star::fromTable('users');

        static::assertSame('users', $star->table());
        static::assertTrue($star->isQualified());
    }

    public function test_roundtrip_plain_star_conversion(): void
    {
        $original = Star::all();
        $node = $original->toAst();
        $reconstructed = Star::fromAst($node);

        static::assertEquals($original->table(), $reconstructed->table());
        static::assertFalse($reconstructed->isQualified());
    }

    public function test_roundtrip_qualified_star_conversion(): void
    {
        $original = Star::fromTable('products');
        $node = $original->toAst();
        $reconstructed = Star::fromAst($node);

        static::assertEquals($original->table(), $reconstructed->table());
        static::assertTrue($reconstructed->isQualified());
    }
}
