<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Partitioning;

use Flow\ETL\API\DataFrame;
use Flow\ETL\Functions\Reference\Reference;

use function Flow\ETL\DSL\ref;

/**
 * Orders fixture gives ~5 (seller) and ~365 (created_at day) partitions; no third fixture exists.
 */
enum PartitionCardinality: string
{
    case high = 'high';
    case low = 'low';

    public function column(): string
    {
        return match ($this) {
            self::high => 'day',
            self::low => 'seller_id',
        };
    }

    /**
     * Every producer derives the key the same way, so the two groups that use this enum cannot end up
     * partitioning by differently-computed columns under one shared label.
     */
    public function derive(DataFrame $frame): DataFrame
    {
        return $frame->withColumn('day', ref('created_at')->dateFormat('Y-m-d'));
    }

    public function reference(): Reference
    {
        return ref($this->column());
    }
}
