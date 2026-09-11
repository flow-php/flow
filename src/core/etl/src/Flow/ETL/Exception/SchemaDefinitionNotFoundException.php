<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Schema\SimilarNames;

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
        private readonly string $entry,
        private readonly array $suggestions = [],
    ) {
        parent::__construct(
            count($this->suggestions) > 0
                ? sprintf(
                    'Schema definition for entry "%s" not found. Did you mean one of: [%s]?',
                    $entry,
                    implode(', ', $this->suggestions),
                )
                : sprintf('Schema definition for entry "%s" not found.', $entry),
        );
    }

    public static function withAvailable(string $entry, string ...$available): self
    {
        return new self($entry, (new SimilarNames())->closestTo($entry, array_values($available)));
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

    public function entry(): string
    {
        return $this->entry;
    }
}
