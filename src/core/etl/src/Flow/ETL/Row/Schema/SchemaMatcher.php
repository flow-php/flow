<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use Flow\ETL\Row\Schema;

interface SchemaMatcher
{
    public function match(Schema $left, Schema $right) : bool;
}
