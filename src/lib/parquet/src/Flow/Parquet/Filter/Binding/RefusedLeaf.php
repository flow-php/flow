<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

/**
 * A leaf no row answers: pruning leaves it Unbound, evaluation refuses it with $reason.
 */
final readonly class RefusedLeaf implements LeafResolution
{
    public function __construct(
        public string $reason,
    ) {}
}
