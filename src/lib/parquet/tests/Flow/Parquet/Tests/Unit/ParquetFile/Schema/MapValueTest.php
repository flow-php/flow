<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema;

use Flow\Parquet\ParquetFile\Schema\MapValue;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class MapValueTest extends TestCase
{
    #[TestWith([false])]
    #[TestWith([true])]
    public function test_decimal_value_keeps_precision_and_scale(bool $required): void
    {
        $value = MapValue::decimal(18, 2, $required)->value;

        static::assertSame(18, $value->logicalType()?->decimalData()?->precision());
        static::assertSame(2, $value->logicalType()?->decimalData()?->scale());
        static::assertSame(8, $value->typeLength());
    }
}
