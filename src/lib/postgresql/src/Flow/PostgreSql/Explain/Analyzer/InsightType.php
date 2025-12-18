<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Analyzer;

enum InsightType : string
{
    case DISK_READ = 'disk_read';
    case ESTIMATE_MISMATCH = 'estimate_mismatch';
    case EXTERNAL_SORT = 'external_sort';
    case INEFFICIENT_FILTER = 'inefficient_filter';
    case LOW_CACHE_HIT = 'low_cache_hit';
    case SEQUENTIAL_SCAN = 'sequential_scan';
    case SLOW_NODE = 'slow_node';
}
