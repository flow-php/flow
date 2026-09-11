<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Partitioning;

use Flow\ETL\DataFrame;
use Flow\ETL\Row\Reference;

use function Flow\ETL\DSL\ref;

/**
 * The orders fixture spreads rows over 5 sellers and a full year of created_at, so these two keys
 * give ~5 and ~365 partitions. There is no synthetic fixture behind a third point and none is invented.
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
        return $frame->withEntry('day', ref('created_at')->dateFormat('Y-m-d'));
    }

    public function reference(): Reference
    {
        return ref($this->column());
    }
}
