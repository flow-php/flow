<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Type;

use Flow\PostgreSql\QueryBuilder\Schema\DataType;
use Flow\PostgreSql\QueryBuilder\Schema\Type\TypeAttribute;
use PHPUnit\Framework\TestCase;

final class TypeAttributeTest extends TestCase
{
    public function test_type_attribute_basic() : void
    {
        $attr = TypeAttribute::of('name', DataType::text());

        self::assertSame('name', $attr->name);
        self::assertInstanceOf(DataType::class, $attr->type);
        self::assertNull($attr->collation);
    }

    public function test_type_attribute_different_types() : void
    {
        $intAttr = TypeAttribute::of('count', DataType::integer());
        $boolAttr = TypeAttribute::of('active', DataType::boolean());
        $numericAttr = TypeAttribute::of('price', DataType::numeric(10, 2));

        self::assertInstanceOf(DataType::class, $intAttr->type);
        self::assertInstanceOf(DataType::class, $boolAttr->type);
        self::assertInstanceOf(DataType::class, $numericAttr->type);
    }

    public function test_type_attribute_immutability() : void
    {
        $original = TypeAttribute::of('name', DataType::text());
        $modified = $original->collate('en_US');

        self::assertNull($original->collation);
        self::assertSame('en_US', $modified->collation);
    }

    public function test_type_attribute_with_collation() : void
    {
        $attr = TypeAttribute::of('name', DataType::text())
            ->collate('en_US');

        self::assertSame('name', $attr->name);
        self::assertInstanceOf(DataType::class, $attr->type);
        self::assertSame('en_US', $attr->collation);
    }
}
