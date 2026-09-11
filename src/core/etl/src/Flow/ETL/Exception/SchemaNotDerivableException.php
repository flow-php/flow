<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use function sprintf;

class SchemaNotDerivableException extends InvalidArgumentException
{
    public static function extractor(string $extractor, ?string $reason = null): self
    {
        if ($reason === null) {
            return new self(sprintf(
                '%s cannot describe what it will produce before producing it. Declare the schema with '
                . '->withSchema(), or read from a source that describes itself.',
                $extractor,
            ));
        }

        return new self(sprintf(
            '%s cannot describe what it will produce before producing it: %s. Declare the schema with '
            . '->withSchema().',
            $extractor,
            $reason,
        ));
    }

    public static function function(string $function, string $reason): self
    {
        return new self(sprintf('%s() cannot describe the column it produces: %s.', $function, $reason));
    }

    public static function nonRewindable(string $extractor): self
    {
        return new self(sprintf(
            '%s cannot read its dataset twice, so discover_pivot_values() cannot scan it before the pivot '
            . 'runs. Declare the values with pivot_values(...).',
            $extractor,
        ));
    }
}
