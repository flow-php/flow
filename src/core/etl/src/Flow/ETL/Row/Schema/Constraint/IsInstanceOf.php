<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Constraint;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Constraint;

final class IsInstanceOf implements Constraint
{
    /**
     * @param class-string $class
     */
    public function __construct(private readonly string $class)
    {
    }

    public function isSatisfiedBy(Entry $entry) : bool
    {
        return $entry->value() instanceof $this->class;
    }
}
