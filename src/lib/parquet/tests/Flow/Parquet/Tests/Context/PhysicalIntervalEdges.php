<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use DateTimeImmutable;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Filter\Binding\LiteralDomain;
use Flow\Parquet\Filter\Binding\PhysicalInterval;
use Flow\Parquet\Filter\Binding\PhysicalIntervals;
use Flow\Parquet\Filter\Operator;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

use function array_map;

final class PhysicalIntervalEdges
{
    /**
     * Each interval as [lower, lowerInclusive, upper, upperInclusive]; null when the intervals are.
     *
     * @return null|list<array{0: null|bool|float|int|string, 1: bool, 2: null|bool|float|int|string, 3: bool}>
     */
    public static function of(?PhysicalIntervals $intervals): ?array
    {
        return $intervals === null
            ? null
            : array_map(static fn(PhysicalInterval $interval): array => [
                $interval->lower,
                $interval->lowerInclusive,
                $interval->upper,
                $interval->upperInclusive,
            ], $intervals->intervals);
    }

    /**
     * The edges of the column's pre-image of `$operator $literal`, through the converter that decodes the column.
     *
     * @return null|list<array{0: null|bool|float|int|string, 1: bool, 2: null|bool|float|int|string, 3: bool}>
     */
    public static function ofComparison(
        FlatColumn $column,
        Operator $operator,
        bool|DateTimeImmutable|float|int|string $literal,
    ): ?array {
        return self::of(self::intervals($column, $operator, $literal));
    }

    public static function intervals(
        FlatColumn $column,
        Operator $operator,
        bool|DateTimeImmutable|float|int|string $literal,
    ): ?PhysicalIntervals {
        [, $preImage] = LiteralDomain::forColumn(
            $column,
            DataConverter::initialize(new Options())->resolveConverter($column),
        );

        return $preImage === null
            ? null
            : PhysicalIntervals::preImage($preImage, PhysicalOrder::forColumn($column), $operator, $literal);
    }
}
