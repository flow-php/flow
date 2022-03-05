<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\Loader;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;
use Flow\ETL\SchemaValidator;

/**
 * @implements Loader<array{schema: Schema, validator: SchemaValidator}>
 */
final class SchemaValidationLoader implements Loader
{
    private Schema $schema;

    private SchemaValidator $validator;

    public function __construct(Schema $schema, SchemaValidator $validator)
    {
        $this->schema = $schema;
        $this->validator = $validator;
    }

    public function __serialize() : array
    {
        return [
            'schema' => $this->schema,
            'validator' => $this->validator,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->schema = $data['schema'];
    }

    public function load(Rows $rows) : void
    {
        if (!$this->validator->isValid($rows, $this->schema)) {
            throw new SchemaValidationException($this->schema, $rows);
        }
    }
}
