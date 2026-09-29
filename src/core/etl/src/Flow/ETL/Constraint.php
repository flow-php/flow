<?php

declare(strict_types=1);

namespace Flow\ETL;

interface Constraint
{
    /**
     * Index of the first row that violates; state advances over the rows before it.
     */
    public function firstViolation(Rows $rows): ?int;

    public function toString(): string;

    public function violation(Rows $rows, int $index): string;
}
