<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Exception;

final class SchemaValidationException extends RuntimeException
{
    public function __construct(string $mismatches)
    {
        parent::__construct("Schema validation failed: \n" . $mismatches);
    }
}
