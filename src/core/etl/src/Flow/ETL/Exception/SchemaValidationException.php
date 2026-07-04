<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Schema;
use Flow\ETL\Schema\Validator\ValidationContext;

final class SchemaValidationException extends RuntimeException
{
    public function __construct(
        private readonly Schema $expected,
        private readonly Schema $given,
        private readonly ValidationContext $context,
    ) {
        parent::__construct("Schema validation failed: \n" . $this->context->toString());
    }

    public function context(): ValidationContext
    {
        return $this->context;
    }

    /**
     * @return Schema
     */
    public function given(): Schema
    {
        return $this->given;
    }

    /**
     * @return Schema
     */
    public function schema(): Schema
    {
        return $this->expected;
    }
}
