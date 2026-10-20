<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\PhysicalIntervalEdges;
use Flow\Parquet\Tests\Mother\OperatorMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class DatePreImageTest extends TestCase
{
    /**
     * @param list<array{0: ?int, 1: bool, 2: ?int, 3: bool}> $expected
     */
    #[TestWith(['Equal', '2024-01-01T00:00:00Z', [[19723, true, 19723, true]]])]
    #[TestWith(['Equal', '2024-01-01T12:00:00Z', []])]
    #[TestWith(['Equal', '2024-01-01T00:00:00+02:00', []])]
    #[TestWith(['GreaterThan', '2023-12-31T19:00:00Z', [[19722, false, null, false]]])]
    #[TestWith(['GreaterThanOrEqual', '2024-01-01T12:00:00Z', [[19724, true, null, false]]])]
    #[TestWith(['LessThan', '2024-01-01T12:00:00Z', [[null, false, 19724, false]]])]
    #[TestWith(['LessThanOrEqual', '2024-01-01T00:00:00Z', [[null, false, 19723, true]]])]
    public function test_a_date_maps_the_literal_utc_instant_to_days(
        string $operator,
        string $literal,
        array $expected,
    ): void {
        static::assertSame($expected, PhysicalIntervalEdges::ofComparison(
            FlatColumn::date('d'),
            OperatorMother::named($operator),
            new DateTimeImmutable($literal),
        ));
    }
}
