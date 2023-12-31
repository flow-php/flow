<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use Flow\ETL\Row\Entry;

interface Constraint
{
    public function isSatisfiedBy(Entry $entry) : bool;
}
