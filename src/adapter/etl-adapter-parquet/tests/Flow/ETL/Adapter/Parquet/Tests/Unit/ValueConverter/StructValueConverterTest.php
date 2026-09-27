<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\ValueConverter;

use Flow\ETL\Adapter\Parquet\ValueConverter\JsonValueConverter;
use Flow\ETL\Adapter\Parquet\ValueConverter\StructValueConverter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;

final class StructValueConverterTest extends FlowTestCase
{
    public function test_encode_stringifies_child_objects(): void
    {
        static::assertSame(
            ['data' => '{"a":1}', 'n' => 1],
            (new StructValueConverter(['data' => new JsonValueConverter()]))->encode([
                'data' => Json::fromArray(['a' => 1]),
                'n' => 1,
            ]),
        );
    }

    public function test_non_array_values_pass_through(): void
    {
        $converter = new StructValueConverter(['data' => new JsonValueConverter()]);

        static::assertSame('scalar', $converter->encode('scalar'));
    }
}
