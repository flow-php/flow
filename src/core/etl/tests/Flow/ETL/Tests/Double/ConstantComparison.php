<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Rows;

use function array_fill;

/**
 * A comparison whose every pair gives the same answer.
 */
final readonly class ConstantComparison implements Comparison
{
    public function __construct(
        public bool $result,
    ) {}

    public function compare(Rows $left, Rows $right): array
    {
        // @mago-ignore analysis:possibly-invalid-argument
        return $left->isEmpty() ? [] : array_fill(0, $left->count(), $this->result);
    }

    public function left(): array
    {
        return [];
    }

    public function right(): array
    {
        return [];
    }
}
