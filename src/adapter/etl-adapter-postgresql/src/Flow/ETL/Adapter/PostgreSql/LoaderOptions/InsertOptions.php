<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\LoaderOptions;

/**
 * Configuration options for INSERT operations in PostgreSQL loader.
 *
 * Supports ON CONFLICT handling for upsert operations.
 */
final readonly class InsertOptions
{
    /**
     * @param bool $skipConflicts If true, use ON CONFLICT DO NOTHING
     * @param list<string> $conflictColumns Column names for ON CONFLICT (columns)
     * @param null|string $conflictConstraint Constraint name for ON CONFLICT ON CONSTRAINT
     * @param list<string> $updateColumns Columns to update on conflict (empty = all non-key columns)
     */
    public function __construct(
        public bool $skipConflicts = false,
        public array $conflictColumns = [],
        public ?string $conflictConstraint = null,
        public array $updateColumns = [],
    ) {}

    /**
     * Create options for skipping conflicting rows (ON CONFLICT DO NOTHING).
     */
    public static function skipConflicts(): self
    {
        return new self(skipConflicts: true);
    }

    /**
     * Create options for upserting on specific columns.
     *
     * @param list<string> $columns Columns to detect conflicts on
     * @param list<string> $updateColumns Columns to update on conflict (empty = all non-key columns)
     */
    public static function upsertOnColumns(array $columns, array $updateColumns = []): self
    {
        return new self(conflictColumns: $columns, updateColumns: $updateColumns);
    }

    /**
     * Create options for upserting on a constraint.
     *
     * @param string $constraintName Name of the constraint to detect conflicts on
     * @param list<string> $updateColumns Columns to update on conflict (empty = all non-key columns)
     */
    public static function upsertOnConstraint(string $constraintName, array $updateColumns = []): self
    {
        return new self(conflictConstraint: $constraintName, updateColumns: $updateColumns);
    }

    public function hasConflictHandling(): bool
    {
        return $this->skipConflicts || $this->conflictColumns !== [] || $this->conflictConstraint !== null;
    }
}
