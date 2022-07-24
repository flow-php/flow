<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Rows;

final class SchemaValidationException extends RuntimeException
{
    public function __construct(private readonly Schema $schema, private readonly Rows $rows)
    {
        $schema = (new ASCIISchemaFormatter())->format($this->schema);
        $rowsSchema = (new ASCIISchemaFormatter())->format($rows->schema());

        parent::__construct(
            <<<SCHEMA
Given schema:
{$schema}
Does not match rows: 
{$rowsSchema}
SCHEMA
        );
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
