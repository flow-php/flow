<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Constraint;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Constraint;

final class SameAs implements Constraint
{
    public function __construct(private readonly mixed $value)
    {
    }

    public function isSatisfiedBy(Entry $entry) : bool
    {
        return $entry->value() === $this->value;
    }
}
