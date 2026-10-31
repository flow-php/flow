<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Exception;

use function array_map;
use function implode;
use function sprintf;

final class SchemaValidationException extends RuntimeException
{
    public function __construct(string $mismatches)
    {
        parent::__construct("Schema validation failed: \n" . $mismatches);
    }

    /**
     * @param list<string> $missing the declared columns $header lacks
     * @param list<string> $header the columns the source carries
     */
    public static function declaredColumnsMissing(string $source, array $missing, array $header): self
    {
        return new self(sprintf(
            'Columns %s declared by the schema are not in the header of %s (%s). Correct their names, or remove them '
            . 'from ->withSchema(...).',
            implode(', ', array_map(static fn(string $column): string => '"' . $column . '"', $missing)),
            $source,
            implode(', ', $header),
        ));
    }
}
