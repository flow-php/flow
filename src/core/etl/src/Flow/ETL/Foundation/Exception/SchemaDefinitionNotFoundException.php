<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Exception;

use Flow\ETL\Foundation\SimilarNames;

use function array_values;
use function count;
use function implode;
use function sprintf;

final class SchemaDefinitionNotFoundException extends InvalidArgumentException
{
    /**
     * @param list<string> $suggestions
     */
    public function __construct(
        private readonly string $column,
        private readonly array $suggestions = [],
    ) {
        parent::__construct(
            count($this->suggestions) > 0
                ? sprintf(
                    'Schema definition for column "%s" not found. Did you mean one of: [%s]?',
                    $column,
                    implode(', ', $this->suggestions),
                )
                : sprintf('Schema definition for column "%s" not found.', $column),
        );
    }

    public static function withAvailable(string $column, string ...$available): self
    {
        return new self($column, (new SimilarNames())->closestTo($column, array_values($available)));
    }

    /**
     * The names closest to the one that was not found, ranked by similarity and capped - not the
     * full column list.
     *
     * @return list<string>
     */
    public function suggestions(): array
    {
        return $this->suggestions;
    }

    public function column(): string
    {
        return $this->column;
    }
}
