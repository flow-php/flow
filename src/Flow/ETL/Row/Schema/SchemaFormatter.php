<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use Flow\ETL\Row\Schema;

interface SchemaFormatter
{
    public function format(Schema $schema) : string;
}
