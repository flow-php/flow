<?php declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Row\Schema;

interface SchemaValidator
{
    public function isValid(Rows $rows, Schema $schema) : bool;
}
