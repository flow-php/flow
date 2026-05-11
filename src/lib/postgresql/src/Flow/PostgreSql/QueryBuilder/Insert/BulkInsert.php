<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Insert;

use Flow\PostgreSql\QueryBuilder\Clause\ConflictTarget;
use Flow\PostgreSql\QueryBuilder\Sql;

/**
 * Optimized bulk INSERT query builder for high-performance multi-row inserts.
 *
 * Unlike InsertBuilder which uses immutable patterns (O(n²) for n rows),
 * this class generates SQL directly using string operations (O(n) complexity).
 *
 * Uses PostgreSQL-style numbered placeholders ($1, $2, $3...).
 */
final readonly class BulkInsert implements Sql
{
    /**
     * @param list<string> $columns
     * @param array<string, string> $updateAssignments Column => Expression pairs for DO UPDATE (e.g., ['name' => 'EXCLUDED.name'])
     */
    private function __construct(
        private string $table,
        private array $columns,
        private int $rowCount,
        private ?ConflictTarget $conflictTarget = null,
        private bool $doNothing = false,
        private array $updateAssignments = [],
    ) {}

    /**
     * @param list<string> $columns
     */
    public static function into(string $table, array $columns, int $rowCount): self
    {
        if ($rowCount < 1) {
            throw new \InvalidArgumentException('Row count must be at least 1');
        }

        if ($columns === []) {
            throw new \InvalidArgumentException('At least one column is required');
        }

        return new self($table, $columns, $rowCount);
    }

    /**
     * ON CONFLICT DO NOTHING - skip rows that would cause conflicts.
     */
    public function onConflictDoNothing(?ConflictTarget $target = null): self
    {
        return new self($this->table, $this->columns, $this->rowCount, $target, true, []);
    }

    /**
     * ON CONFLICT DO UPDATE - update existing rows on conflict.
     *
     * @param null|list<string> $updateColumns Columns to update. If null, updates all non-conflict columns.
     */
    public function onConflictDoUpdate(ConflictTarget $target, ?array $updateColumns = null): self
    {
        $columnsToUpdate = $updateColumns ?? \array_diff($this->columns, $target->getColumns());
        $assignments = [];

        foreach ($columnsToUpdate as $column) {
            $assignments[$column] = 'EXCLUDED."' . $column . '"';
        }

        return new self($this->table, $this->columns, $this->rowCount, $target, false, $assignments);
    }

    public function toSql(): string
    {
        $columnCount = \count($this->columns);
        $quotedColumns = \implode(', ', \array_map(static fn(string $col): string => '"' . $col . '"', $this->columns));

        $rows = [];

        for ($r = 0; $r < $this->rowCount; $r++) {
            $offset = $r * $columnCount;
            $placeholders = [];

            for ($c = 1; $c <= $columnCount; $c++) {
                $placeholders[] = '$' . ($offset + $c);
            }

            $rows[] = '(' . \implode(', ', $placeholders) . ')';
        }

        $sql = \sprintf('INSERT INTO "%s" (%s) VALUES %s', $this->table, $quotedColumns, \implode(', ', $rows));

        if ($this->doNothing) {
            $sql .= ' ON CONFLICT';

            if ($this->conflictTarget !== null) {
                $sql .= ' ' . $this->formatConflictTarget($this->conflictTarget);
            }

            $sql .= ' DO NOTHING';
        } elseif ($this->updateAssignments !== []) {
            $sql .= ' ON CONFLICT';

            if ($this->conflictTarget !== null) {
                $sql .= ' ' . $this->formatConflictTarget($this->conflictTarget);
            }

            $updates = [];

            foreach ($this->updateAssignments as $column => $expression) {
                $updates[] = '"' . $column . '" = ' . $expression;
            }

            $sql .= ' DO UPDATE SET ' . \implode(', ', $updates);
        }

        return $sql;
    }

    private function formatConflictTarget(ConflictTarget $target): string
    {
        $constraint = $target->getConstraint();

        if ($constraint !== null) {
            return 'ON CONSTRAINT "' . $constraint . '"';
        }

        $columns = $target->getColumns();

        if ($columns !== []) {
            $quotedColumns = \array_map(static fn(string $col): string => '"' . $col . '"', $columns);

            return '(' . \implode(', ', $quotedColumns) . ')';
        }

        return '';
    }
}
