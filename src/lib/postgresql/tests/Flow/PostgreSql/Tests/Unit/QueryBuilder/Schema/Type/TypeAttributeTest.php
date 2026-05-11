<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Type;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Schema\Type\TypeAttribute;
use PHPUnit\Framework\TestCase;

final class TypeAttributeTest extends TestCase
{
    public function test_type_attribute_basic(): void
    {
        $attr = TypeAttribute::of('name', ColumnType::text());

        static::assertSame('name', $attr->name);
        static::assertInstanceOf(ColumnType::class, $attr->type);
        static::assertNull($attr->collation);
    }

    public function test_type_attribute_different_types(): void
    {
        $intAttr = TypeAttribute::of('count', ColumnType::integer());
        $boolAttr = TypeAttribute::of('active', ColumnType::boolean());
        $numericAttr = TypeAttribute::of('price', ColumnType::numeric(10, 2));

        static::assertInstanceOf(ColumnType::class, $intAttr->type);
        static::assertInstanceOf(ColumnType::class, $boolAttr->type);
        static::assertInstanceOf(ColumnType::class, $numericAttr->type);
    }

    public function test_type_attribute_immutability(): void
    {
        $original = TypeAttribute::of('name', ColumnType::text());
        $modified = $original->collate('en_US');

        static::assertNull($original->collation);
        static::assertSame('en_US', $modified->collation);
    }

    public function test_type_attribute_with_collation(): void
    {
        $attr = TypeAttribute::of('name', ColumnType::text())->collate('en_US');

        static::assertSame('name', $attr->name);
        static::assertInstanceOf(ColumnType::class, $attr->type);
        static::assertSame('en_US', $attr->collation);
    }
}
