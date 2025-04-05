<?php

declare(strict_types=1);

namespace Flow\Calculator\Tests\Unit;

use Flow\Calculator\Exception\{InvalidScaleException, NonNumericValueException};
use Flow\Calculator\NumberNormalizer;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

final class NumberNormalizerTest extends FlowTestCase
{
    public function test_normalize_invalid_numbers_to_string() : void
    {
        $this->expectException(NonNumericValueException::class);
        $this->expectExceptionMessage('foo');

        NumberNormalizer::toString('foo', scale: 0);
    }

    #[TestWith(['1', 6, '1'])]
    #[TestWith([1, 1, '1'])]
    #[TestWith([1.0, 1, '1'])]
    #[TestWith([1.1, 1, '1.1'])]
    #[TestWith([1.123456, 5, '1.12346'])]
    #[TestWith([1.1234568, 6, '1.123457'])]
    #[TestWith([1.1234568, 7, '1.1234568'])]
    #[TestWith([1.1234568, 9, '1.1234568'])]
    #[TestWith([3.23E-5, 6, '0.000032'])]
    #[TestWith([3.23E-5, 7, '0.0000323'])]
    #[TestWith([3.23E-5, 8, '0.0000323'])]
    #[TestWith(['3.23E-5', 12, '0.0000323'])]
    #[TestWith(['3.23e-5', 12, '0.0000323'])]
    #[TestWith([3.23E-5, 12, '0.0000323'])]
    public function test_normalize_numbers_to_string(int|string|float $input, int $scale, string $output) : void
    {
        self::assertSame($output, NumberNormalizer::toString($input, scale: $scale));
    }

    public function test_using_invalid_scale() : void
    {
        $this->expectException(InvalidScaleException::class);
        $this->expectExceptionMessage('Scale "17" is invalid. It must be between 0 and 16.');

        NumberNormalizer::toString(1, scale: 17);
    }
}
