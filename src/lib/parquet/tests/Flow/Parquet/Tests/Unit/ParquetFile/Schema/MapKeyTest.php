<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema;

use Flow\Parquet\ParquetFile\Schema\MapKey;
use PHPUnit\Framework\TestCase;

final class MapKeyTest extends TestCase
{
    public function test_decimal_key_keeps_precision_and_scale(): void
    {
        $key = MapKey::decimal(18, 2)->key;

        static::assertSame(18, $key->logicalType()?->decimalData()?->precision());
        static::assertSame(2, $key->logicalType()?->decimalData()?->scale());
        static::assertSame(8, $key->typeLength());
    }
}
