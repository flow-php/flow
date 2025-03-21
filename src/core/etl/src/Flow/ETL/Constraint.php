<?php

declare(strict_types=1);

namespace Flow\ETL;

interface Constraint
{
    public function isSatisfiedBy(Row $row) : bool;

    public function toString() : string;

    public function violation(Row $row) : string;
}
