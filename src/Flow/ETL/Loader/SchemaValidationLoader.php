<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;
use Flow\ETL\SchemaValidator;

/**
 * @implements Loader<array{schema: Schema, validator: SchemaValidator}>
 */
final class SchemaValidationLoader implements Loader
{
    public function __construct(
        private readonly Schema $schema,
        private readonly SchemaValidator $validator
    ) {
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
        $this->validator = $data['validator'];
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$this->validator->isValid($rows, $this->schema)) {
            throw new SchemaValidationException($this->schema, $rows);
        }
    }
}
