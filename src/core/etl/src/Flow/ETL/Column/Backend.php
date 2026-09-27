<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\ETL\Schema\Definition;

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
}
