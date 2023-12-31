<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;
use Flow\ETL\SchemaValidator;

final class SchemaValidationLoader implements Loader
{
    public function __construct(
        private readonly Schema $schema,
        private readonly SchemaValidator $validator
    ) {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$this->validator->isValid($rows, $this->schema)) {
            throw new SchemaValidationException($this->schema, $rows);
        }
    }
}
