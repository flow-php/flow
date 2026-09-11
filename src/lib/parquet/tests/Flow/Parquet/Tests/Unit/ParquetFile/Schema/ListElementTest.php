<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema;

use Flow\Parquet\ParquetFile\Schema\ListElement;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class ListElementTest extends TestCase
{
    #[TestWith([false])]
    #[TestWith([true])]
    public function test_decimal_element_keeps_precision_and_scale(bool $required): void
    {
        $element = ListElement::decimal(18, 2, $required)->element;

        static::assertSame(18, $element->logicalType()?->decimalData()?->precision());
        static::assertSame(2, $element->logicalType()?->decimalData()?->scale());
        static::assertSame(8, $element->typeLength());
    }
}
