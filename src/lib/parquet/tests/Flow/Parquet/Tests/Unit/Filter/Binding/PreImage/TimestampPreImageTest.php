<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Filter\Operator;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\Tests\Context\PhysicalIntervalEdges;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use Flow\Parquet\Tests\Mother\OperatorMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class TimestampPreImageTest extends TestCase
{
    /**
     * @param list<array{0: ?int, 1: bool, 2: ?int, 3: bool}> $expected
     */
    #[TestWith(['Equal', '1970-01-01T00:00:00.000001Z', [[1_000, true, 1_999, true]]])]
    #[TestWith(['LessThanOrEqual', '1970-01-01T00:00:00.000001Z', [[null, false, 1_999, true]]])]
    #[TestWith(['GreaterThan', '1970-01-01T00:00:00.000001Z', [[1_999, false, null, false]]])]
    #[TestWith(['LessThan', '1969-12-31T23:59:59.999999Z', [[null, false, -1_000, false]]])]
    #[TestWith(['Equal', '2300-01-01T00:00:00Z', []])]
    #[TestWith(['LessThan', '2300-01-01T00:00:00Z', [[null, false, null, false]]])]
    public function test_nanos_decode_floored_to_micros(string $operator, string $literal, array $expected): void
    {
        static::assertSame($expected, PhysicalIntervalEdges::ofComparison(
            FlatColumnMother::timestamp(TimeUnit::NANOSECONDS),
            OperatorMother::named($operator),
            new DateTimeImmutable($literal),
        ));
    }

    /**
     * @param list<array{0: ?int, 1: bool, 2: ?int, 3: bool}> $expected
     */
    #[TestWith(['Equal', '1970-01-01T00:00:00.0015Z', []])]
    #[TestWith(['Equal', '1970-01-01T00:00:00.002Z', [[2, true, 2, true]]])]
    #[TestWith(['GreaterThan', '1970-01-01T00:00:00.0015Z', [[1, false, null, false]]])]
    #[TestWith(['GreaterThanOrEqual', '1970-01-01T00:00:00.0015Z', [[2, true, null, false]]])]
    #[TestWith(['LessThanOrEqual', '1970-01-01T00:00:00.0015Z', [[null, false, 1, true]]])]
    public function test_millis_hit_a_micros_literal_only_on_the_milli_grid(
        string $operator,
        string $literal,
        array $expected,
    ): void {
        static::assertSame($expected, PhysicalIntervalEdges::ofComparison(
            FlatColumnMother::timestamp(TimeUnit::MILLISECONDS),
            OperatorMother::named($operator),
            new DateTimeImmutable($literal),
        ));
    }

    public function test_micros_are_the_literal_itself(): void
    {
        static::assertSame(
            [[1_500, true, 1_500, true]],
            PhysicalIntervalEdges::ofComparison(
                FlatColumn::dateTime('ts'),
                Operator::Equal,
                new DateTimeImmutable('1970-01-01T00:00:00.0015Z'),
            ),
        );
    }
}
