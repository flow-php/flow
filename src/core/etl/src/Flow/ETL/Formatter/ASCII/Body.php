<?php

declare(strict_types=1);

namespace Flow\ETL\Formatter\ASCII;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

final readonly class Body
{
    public function __construct(
        private Rows $rows,
    ) {}

    public function schema(): Schema
    {
        return $this->rows->schema();
    }

    /**
     * @return int<0, max>
     */
    public function maximumLength(string $column, int|bool $truncate = 20): int
    {
        $definition = $this->schema()->findDefinition($column);

        if ($definition === null) {
            return 0;
        }

        $max = 0;

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->rows->column($definition->reference()->name())->values() as $cell) {
            $value = new ASCIIValue($definition->type(), $cell);

            if ($value->length($truncate) >= $max) {
                $max = $value->length($truncate);
            }
        }

        return $max;
    }

    public function count(): int
    {
        return $this->rows->count();
    }

    /**
     * @throws InvalidArgumentException the batch has no $column column
     */
    public function value(string $column, int $index): mixed
    {
        return $this->rows->column($column)->value($index);
    }
}
