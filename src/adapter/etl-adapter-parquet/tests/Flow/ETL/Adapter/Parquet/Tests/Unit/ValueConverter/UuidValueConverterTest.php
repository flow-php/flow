<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\ValueConverter;

use Flow\ETL\Adapter\Parquet\ValueConverter\UuidValueConverter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid;

final class UuidValueConverterTest extends FlowTestCase
{
    public function test_encode_passes_scalar_through(): void
    {
        static::assertSame(
            'f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a',
            (new UuidValueConverter())->encode('f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a'),
        );
        static::assertNull((new UuidValueConverter())->encode(null));
    }

    public function test_encode_stringifies_uuid_object(): void
    {
        static::assertSame(
            'f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a',
            (new UuidValueConverter())->encode(Uuid::fromString('f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a')),
        );
    }
}
