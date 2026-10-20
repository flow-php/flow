<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding\PreImage;

use Flow\Parquet\Filter\Binding\PreImage\BytesPreImage;
use Flow\Parquet\Filter\Operator;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\PhysicalIntervalEdges;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class BytesPreImageTest extends TestCase
{
    public function test_a_string_binds_its_bytes_and_json_has_no_pre_image(): void
    {
        static::assertSame(
            [[null, false, 'b', false]],
            PhysicalIntervalEdges::ofComparison(FlatColumn::string('s'), Operator::LessThan, 'b'),
        );
        static::assertNull(PhysicalIntervalEdges::intervals(FlatColumn::json('j'), Operator::Equal, '{}'));
    }

    /**
     * @param list<array{0: ?string, 1: bool, 2: ?string, 3: bool}> $expected
     */
    #[TestWith(['ab', [['ab', true, 'ac', false]]])]
    #[TestWith(["a\xFF", [["a\xFF", true, 'b', false]]])]
    #[TestWith(["\xFF\xFF", [["\xFF\xFF", true, null, false]]])]
    #[TestWith(['', [[null, false, null, false]]])]
    public function test_a_prefix_is_the_range_up_to_its_increment(string $prefix, array $expected): void
    {
        static::assertSame($expected, PhysicalIntervalEdges::of((new BytesPreImage())->startsWith($prefix)));
    }
}
