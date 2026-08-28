<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use function count;
use function implode;
use function sprintf;

final class SchemaDefinitionNotFoundException extends InvalidArgumentException
{
    /**
     * @param list<string> $available
     */
    public function __construct(
        private readonly string $entry,
        private readonly array $available = [],
    ) {
        parent::__construct(
            count($this->available) > 0
                ? sprintf(
                    'Schema definition for entry "%s" not found. Available columns: [%s].',
                    $entry,
                    implode(', ', $this->available),
                )
                : sprintf('Schema definition for entry "%s" not found', $entry),
        );
    }

    public static function withAvailable(string $entry, string ...$available): self
    {
        return new self($entry, array_values($available));
    }

    /**
     * @return list<string>
     */
    public function available(): array
    {
        return $this->available;
    }

    public function entry(): string
    {
        return $this->entry;
    }
}
