<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\ValueConverter;

use Flow\ETL\Adapter\Parquet\ValueConverter\ElementsValueConverter;
use Flow\ETL\Adapter\Parquet\ValueConverter\JsonValueConverter;
use Flow\ETL\Adapter\Parquet\ValueConverter\UuidValueConverter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;

final class ElementsValueConverterTest extends FlowTestCase
{
    public function test_encode_stringifies_each_list_element(): void
    {
        static::assertSame(
            ['f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a', null],
            (new ElementsValueConverter(new UuidValueConverter()))->encode([
                Uuid::fromString('f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a'),
                null,
            ]),
        );
    }

    public function test_encode_stringifies_keyed_values_keeping_keys(): void
    {
        static::assertSame(
            ['a' => '{"x":1}'],
            (new ElementsValueConverter(new JsonValueConverter()))->encode(['a' => Json::fromArray(['x' => 1])]),
        );
    }

    public function test_non_array_values_pass_through(): void
    {
        $converter = new ElementsValueConverter(new UuidValueConverter());

        static::assertSame('scalar', $converter->encode('scalar'));
    }
}
