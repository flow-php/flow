<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;

final class SchemaValidationException extends RuntimeException
{
    public function __construct(private readonly Schema $schema, private readonly Rows $rows)
    {
        parent::__construct('Row does not match schema.');
    }

    public function rows() : Rows
    {
        return $this->rows;
    }

    public function schema() : Schema
    {
        return $this->schema;
    }
}
