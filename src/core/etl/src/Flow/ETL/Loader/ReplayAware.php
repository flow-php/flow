<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

/**
 * Answers whether this loader can safely be offered the same Rows batch again (per-batch retries). A loader that
 * holds state across load() calls that cannot be rewound must answer false - Flow cannot determine from outside
 * whether re-offering a batch is safe.
 *
 * @internal
 */
interface ReplayAware
{
    public function replaySafe(): bool;
}
