<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\Row\Schema;
use Flow\ETL\{FlowContext, Loader, Rows, SchemaValidator};

final readonly class SchemaValidationLoader implements Loader
{
    public function __construct(
        private Schema $schema,
        private SchemaValidator $validator,
    ) {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$this->validator->isValid($rows, $this->schema)) {
            throw new SchemaValidationException($this->schema, $rows);
        }
    }
}
