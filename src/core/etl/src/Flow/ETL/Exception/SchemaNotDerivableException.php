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
            '%s cannot read its dataset twice, so describing it would consume the rows before they '
            . 'are extracted. Pass an array, or declare the schema with ->withSchema().',
            $extractor,
        ));
    }

    public static function pipeline(string $extractor): self
    {
        return new self(sprintf(
            '%s holds a whole pipeline, so its output columns depend on every step in it. Reading '
            . 'the schema would have to run the pipeline, which is what asking before extraction avoids.',
            $extractor,
        ));
    }
}
