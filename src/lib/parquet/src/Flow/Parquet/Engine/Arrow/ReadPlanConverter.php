<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine\Arrow;

use Flow\Parquet\Reader\ReadPlan;

use function array_map;

final class ReadPlanConverter
{
    /**
     * Called by name from arrow-ext RustBatchReader::__construct() and RustColumnsReader::__construct(), pinned by
     * phpt 055.
     *
     * @return array{selectors: list<array{0: int, 1: bool}>} file-absolute selectors: row count, skip
     */
    public static function toExtension(ReadPlan $plan): array
    {
        return [
            'selectors' => array_map(static fn($selector): array => [
                $selector->rowCount,
                $selector->skip,
            ], $plan->selection->selectors()),
        ];
    }
}
