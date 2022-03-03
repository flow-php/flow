<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Row;
use Flow\ETL\Row\Schema;

final class SchemaValidationException extends RuntimeException
{
    /**
     * @var Row
     */
    private Row $row;

    /**
     * @var Schema
     */
    private Schema $schema;

    public function __construct(Schema $schema, Row $row)
    {
        $this->schema = $schema;
        $this->row = $row;

        parent::__construct('Row does not match schema.');
    }

    /**
     * @return Row
     */
    public function row() : Row
    {
        return $this->row;
    }

    /**
     * @return Schema
     */
    public function schema() : Schema
    {
        return $this->schema;
    }
}
