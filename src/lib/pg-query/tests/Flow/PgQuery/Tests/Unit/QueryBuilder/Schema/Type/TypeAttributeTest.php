<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Schema\Type;

use Flow\PgQuery\QueryBuilder\Schema\Type\TypeAttribute;
use PHPUnit\Framework\TestCase;

final class TypeAttributeTest extends TestCase
{
    public function test_type_attribute_basic() : void
    {
        $attr = TypeAttribute::of('name', 'text');

        self::assertSame('name', $attr->name);
        self::assertSame('text', $attr->type);
        self::assertNull($attr->collation);
    }

    public function test_type_attribute_different_types() : void
    {
        $intAttr = TypeAttribute::of('count', 'integer');
        $boolAttr = TypeAttribute::of('active', 'boolean');
        $numericAttr = TypeAttribute::of('price', 'numeric(10,2)');

        self::assertSame('integer', $intAttr->type);
        self::assertSame('boolean', $boolAttr->type);
        self::assertSame('numeric(10,2)', $numericAttr->type);
    }

    public function test_type_attribute_immutability() : void
    {
        $original = TypeAttribute::of('name', 'text');
        $modified = $original->collate('en_US');

        self::assertNull($original->collation);
        self::assertSame('en_US', $modified->collation);
    }

    public function test_type_attribute_with_collation() : void
    {
        $attr = TypeAttribute::of('name', 'text')
            ->collate('en_US');

        self::assertSame('name', $attr->name);
        self::assertSame('text', $attr->type);
        self::assertSame('en_US', $attr->collation);
    }
}
