<?php

declare(strict_types=1);

namespace Flow\ETL\Schema;

use Flow\ETL\Schema;

interface SchemaFormatter
{
    public function format(Schema $schema) : string;
}
