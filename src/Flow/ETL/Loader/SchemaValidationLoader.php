<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;

final class SchemaValidationLoader implements Loader
{
    private Schema $schema;

    public function __construct(Schema $schema)
    {
        $this->schema = $schema;
    }

    /**
     * @return array{schema: Schema}
     */
    public function __serialize() : array
    {
        return [
            'schema' => $this->schema,
        ];
    }

    /**
     * @param array{schema: Schema} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->schema = $data['schema'];
    }

    public function load(Rows $rows) : void
    {
        /** @psalm-var pure-callable(Row $row) : void $validator */
        $validator = function (Row $row) : void {
            if (!$this->schema->isValid($row)) {
                throw new SchemaValidationException($this->schema, $row);
            }
        };

        /** @psalm-suppress UnusedMethodCall */
        $rows->each($validator);
    }
}
