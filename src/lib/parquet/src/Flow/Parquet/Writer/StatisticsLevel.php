<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

/**
 * Which statistics a writer computes, as arrow-rs EnabledStatistics: none, column chunk statistics, or column chunk
 * statistics plus a column index entry per page.
 */
enum StatisticsLevel
{
    case CHUNK;
    case NONE;
    case PAGE;
}
