<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\ValueConverter;

use Flow\ETL\Adapter\Parquet\ValueConverter\JsonValueConverter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;

final class JsonValueConverterTest extends FlowTestCase
{
    public function test_encode_passes_scalar_through(): void
    {
        static::assertSame('{"a":1}', (new JsonValueConverter())->encode('{"a":1}'));
        static::assertNull((new JsonValueConverter())->encode(null));
    }

    public function test_encode_stringifies_json_object(): void
    {
        static::assertSame('{"a":1}', (new JsonValueConverter())->encode(Json::fromArray(['a' => 1])));
    }
}
