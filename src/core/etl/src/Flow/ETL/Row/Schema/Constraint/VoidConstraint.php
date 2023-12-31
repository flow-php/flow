<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Constraint;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Constraint;

final class VoidConstraint implements Constraint
{
    public function isSatisfiedBy(Entry $entry) : bool
    {
        return true;
    }
}
