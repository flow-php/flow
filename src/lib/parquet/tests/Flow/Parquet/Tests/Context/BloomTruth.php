<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Evaluation\BloomEvaluator;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Filter\Truth;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Double\InMemoryIndexes;

/**
 * The bloom truth of a filter over row group 0 of columns `i` (INT64), `f` (DOUBLE) and `s` (STRING).
 */
final class BloomTruth
{
    public static function of(Filter $filter, InMemoryIndexes $indexes): Truth
    {
        return BloomEvaluator::evaluate(
            BoundFilter::bind(
                $filter,
                Schema::with(FlatColumn::int64('i'), FlatColumn::double('f'), FlatColumn::string('s')),
                new Options(),
            ),
            $indexes,
            0,
        );
    }
}
