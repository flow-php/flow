<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

/**
 * @template T
 */
interface ExecutionOrderStrategy
{
    /**
     * @param list<T> $items
     *
     * @return list<T>
     */
    public function order(array $items): array;
}
