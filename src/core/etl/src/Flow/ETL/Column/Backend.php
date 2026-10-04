<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Schema\Definition;

use function interface_exists;

if (interface_exists(Backend::class, false)) {
    return;
}

interface Backend
{
    /**
     * @param Definition<mixed> $definition nullability, the enum class, the datetime zone and the cast plan
     */
    public function builder(Definition $definition): ColumnBuilder;

    /**
     * $value is logical or raw and is cast once through the Definition exactly like ColumnBuilder::append().
     *
     * @param Definition<mixed> $definition
     */
    public function constant(Definition $definition, mixed $value, int $count): Column;

    /**
     * Child node lengths and null counts are derived from the buffers, so only the top node travels.
     *
     * @param Definition<mixed> $definition
     * @param list<string> $buffers the column's subtree buffers as Column::encode() produced them ('' = omitted validity)
     */
    public function decode(Definition $definition, array $buffers, int $count, int $nullCount): Column;

    /**
     * The same values in this backend's storage: $column itself when this backend owns it, otherwise one copy through
     * builder($definition)->appendTake(). For producers that build columns outside the configured backend.
     *
     * @param Definition<mixed> $definition
     *
     * @throws ColumnMismatchException a null under NOT NULL on the copy path (an own column passes through unchecked;
     *                                 Rows::fromColumns checks NOT NULL)
     */
    public function adopt(Definition $definition, Column $column): Column;

    /**
     * Bytes this backend holds outside PHP's memory manager (invisible to memory_get_usage() and memory_limit).
     */
    public function allocatedBytes(): int;
}
