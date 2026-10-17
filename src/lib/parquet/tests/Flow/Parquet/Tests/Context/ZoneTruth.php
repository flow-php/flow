<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Evaluation\StatisticsEvaluator;
use Flow\Parquet\Filter\Evaluation\ZoneMap;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Filter\Truth;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Double\ColumnZoneMaps;

/**
 * The truth of a filter over one unit whose columns are `i` (INT64), `f` (DOUBLE) and `s` (STRING).
 */
final class ZoneTruth
{
    /**
     * @param array<string, ZoneMap> $zones
     */
    public static function of(Filter $filter, array $zones): Truth
    {
        return StatisticsEvaluator::evaluate(
            BoundFilter::bind(
                $filter,
                Schema::with(FlatColumn::int64('i'), FlatColumn::double('f'), FlatColumn::string('s')),
                new Options(),
            ),
            new ColumnZoneMaps($zones),
        );
    }
}
