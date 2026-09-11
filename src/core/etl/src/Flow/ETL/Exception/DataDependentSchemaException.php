<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use function sprintf;

final class DataDependentSchemaException extends SchemaNotDerivableException
{
    public static function step(string $step, string $reason): self
    {
        return new self(sprintf('%s cannot describe its output before rows flow: %s.', $step, $reason));
    }
}
