<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;

/**
 * A source that can read fewer rows when the plan has a LIMIT above it. This is an optimization
 * hint, never a guarantee: the limit operator stays in the plan and enforces the exact count, so an
 * implementation that yields more rows than asked - or ignores the hint - is still correct.
 */
interface LimitPushDown
{
    /**
     * Narrowing only. A second push may lower the cap, never raise it.
     *
     * @throws InvalidArgumentException when $limit is not greater than 0
     */
    public function pushLimit(int $limit): void;

    public function pushedLimit(): ?int;
}
