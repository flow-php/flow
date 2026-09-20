<?php

declare(strict_types=1);

namespace Flow\ETL;

use Throwable;

interface Transaction
{
    public function begin(): void;

    public function commit(): void;

    /**
     * Must NOT throw and must not mask $cause - the in-flight failure is the actionable one.
     */
    public function rollback(Throwable $cause): void;
}
