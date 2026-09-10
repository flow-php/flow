<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

/**
 * The sources that can run the canonical orders pipeline from a local fixture. Sources needing a
 * live connection are ServiceSource; sources with no orders shape at all are NodeSource.
 */
enum Source: string
{
    case array = 'array';
    case csv = 'csv';
    case excel = 'excel';
    case floe = 'floe';
    case json = 'json';
    case json_lines = 'json_lines';
    case memory = 'memory';
    case parquet = 'parquet';
}
