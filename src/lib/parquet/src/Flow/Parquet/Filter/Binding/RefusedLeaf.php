<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

/**
 * A leaf no row answers: pruning reads it as Maybe, evaluation refuses it with $reason.
 */
final readonly class RefusedLeaf
{
    public function __construct(
        public string $reason,
    ) {}
}
