<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Formatter\Schema;

use Flow\ETL\Values\Schema;

interface SchemaFormatter
{
    public function format(Schema $schema): string;
}
