<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

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
