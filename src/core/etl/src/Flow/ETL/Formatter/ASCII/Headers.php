<?php

declare(strict_types=1);

namespace Flow\ETL\Formatter\ASCII;

use Countable;
use Flow\ETL\Rows;

use function count;

final class Headers implements Countable
{
    public function __construct(
        private readonly Rows $rows,
    ) {}

    public function count(): int
    {
        return count($this->names());
    }

    /**
     * @return array<string>
     */
    public function names(): array
    {
        return $this->rows->schema()->references()->names();
    }
}
