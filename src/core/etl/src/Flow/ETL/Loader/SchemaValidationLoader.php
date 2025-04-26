<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\{FlowContext, Loader, Rows, SchemaValidator};
use Flow\ETL\Schema;

final readonly class SchemaValidationLoader implements Loader
{
    public function __construct(
        private Schema $expected,
        private SchemaValidator $validator,
    ) {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $given = $rows->schema();

        if (!$this->validator->isValid($given, $this->expected)) {
            throw new SchemaValidationException($this->expected, $given);
        }
    }
}
