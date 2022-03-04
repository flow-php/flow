<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;

final class SchemaValidationException extends RuntimeException
{
    private Rows $rows;

    private Schema $schema;

    public function __construct(Schema $schema, Rows $rows)
    {
        $this->schema = $schema;
        $this->rows = $rows;

        parent::__construct('Row does not match schema.');
    }

    public function rows() : Rows
    {
        return $this->rows;
    }

    /**
     * @return Schema
     */
    public function schema() : Schema
    {
        return $this->schema;
    }
}
