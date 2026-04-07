<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

/**
 * @template T
 *
 * @implements ExecutionOrderStrategy<T>
 */
final readonly class NoExecutionOrder implements ExecutionOrderStrategy
{
    public function order(array $items) : array
    {
        return $items;
    }
}
