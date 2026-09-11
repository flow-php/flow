<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder;

use function count;
use function str_contains;
use function str_ends_with;
use function str_starts_with;
use function strlen;
use function substr;

/**
 * Parses and represents qualified SQL identifiers like schema.table or table.column.
 *
 * Handles dot notation parsing while respecting double-quoted identifiers.
 * Examples:
 * - "users" → parts: ["users"]
 * - "public.users" → parts: ["public", "users"]
 * - '"my.table"' → parts: ["my.table"] (quoted, not split)
 * - 'public."my.table"' → parts: ["public", "my.table"]
 * - "schema.table.column" → parts: ["schema", "table", "column"]
 */
final readonly class QualifiedIdentifier
{
    /**
     * @param non-empty-list<string> $parts
     */
    private function __construct(
        private array $parts,
    ) {}

    /**
     * Create from explicit parts without parsing.
     *
     * @param non-empty-list<string> $parts
     */
    public static function fromParts(array $parts): self
    {
        return new self($parts);
    }

    /**
     * Parse a qualified identifier string, respecting double-quoted parts.
     */
    public static function parse(string $identifier): self
    {
        if ($identifier === '') {
            return new self(['']);
        }

        $parts = self::splitRespectingQuotes($identifier);

        return new self($parts);
    }

    /**
     * For three-part identifiers (schema.table.column), get the column part (last).
     * For two-part identifiers (table.column), get the column part (last).
     * For single-part identifiers, get the only part.
     */
    public function column(): string
    {
        return $this->parts[count($this->parts) - 1];
    }

    /**
     * Get the number of parts.
     */
    public function count(): int
    {
        return count($this->parts);
    }

    /**
     * Check if this identifier has a schema part.
     *
     * @assert-if-true !null $this->schema()
     */
    public function hasSchema(): bool
    {
        return count($this->parts) >= 2;
    }

    /**
     * Get the last part (typically the name/column).
     */
    public function name(): string
    {
        return $this->parts[count($this->parts) - 1];
    }

    /**
     * Get all parts of the identifier.
     *
     * @return non-empty-list<string>
     */
    public function parts(): array
    {
        return $this->parts;
    }

    /**
     * Get the schema part for a two-part identifier (schema.name).
     * Returns null if there's only one part.
     */
    public function schema(): ?string
    {
        if (count($this->parts) < 2) {
            return null;
        }

        return $this->parts[0];
    }

    /**
     * For three-part identifiers (schema.table.column), get the table part.
     * For two-part identifiers (table.column), get the first part.
     * Returns null for single-part identifiers.
     */
    public function table(): ?string
    {
        if (count($this->parts) === 3) {
            return $this->parts[1];
        }

        if (count($this->parts) === 2) {
            return $this->parts[0];
        }

        return null;
    }

    /**
     * Split a qualified identifier by dots, but respect double-quoted parts.
     *
     * @return non-empty-list<string>
     */
    private static function splitRespectingQuotes(string $identifier): array
    {
        if (str_starts_with($identifier, '"') && str_ends_with($identifier, '"')) {
            $inner = substr($identifier, 1, -1);

            if (str_contains($inner, '"')) {
            } else {
                return [$inner];
            }
        }

        $parts = [];
        $current = '';
        $inQuotes = false;
        $length = strlen($identifier);

        for ($i = 0; $i < $length; $i++) {
            $char = $identifier[$i];

            if ($char === '"') {
                $inQuotes = !$inQuotes;
            } elseif ($char === '.' && !$inQuotes) {
                $parts[] = $current;
                $current = '';
            } else {
                $current .= $char;
            }
        }

        $parts[] = $current;

        return $parts;
    }
}
