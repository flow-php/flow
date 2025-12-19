<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use function Flow\PostgreSql\DSL\{bulk_insert, col, cond_and, conflict_columns, conflict_constraint, delete, eq, literal, update};
use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\{DeleteOptions, InsertOptions, UpdateOptions};
use Flow\ETL\{FlowContext, Loader, Row, Rows};
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\{XMLElementEntry, XMLEntry};
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\Condition\Comparison;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Insert\BulkInsert;

/**
 * PostgreSQL loader for ETL pipelines.
 *
 * Supports INSERT, UPDATE, and DELETE operations.
 */
final class PostgreSqlLoader implements Loader
{
    private ?DeleteOptions $deleteOptions = null;

    private ?InsertOptions $insertOptions = null;

    private Operation $operation = Operation::INSERT;

    private ?UpdateOptions $updateOptions = null;

    public function __construct(
        private readonly Client $client,
        private readonly string $table,
    ) {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if ($rows->count() === 0) {
            return;
        }

        match ($this->operation) {
            Operation::INSERT => $this->insertRows($rows),
            Operation::UPDATE => $this->updateRows($rows),
            Operation::DELETE => $this->deleteRows($rows),
        };
    }

    public function withDeleteOptions(DeleteOptions $options) : self
    {
        $loader = clone $this;
        $loader->deleteOptions = $options;

        return $loader;
    }

    public function withInsertOptions(InsertOptions $options) : self
    {
        $loader = clone $this;
        $loader->insertOptions = $options;

        return $loader;
    }

    public function withOperation(Operation $operation) : self
    {
        $loader = clone $this;
        $loader->operation = $operation;

        return $loader;
    }

    public function withUpdateOptions(UpdateOptions $options) : self
    {
        $loader = clone $this;
        $loader->updateOptions = $options;

        return $loader;
    }

    private function applyBulkConflictHandling(BulkInsert $query) : BulkInsert
    {
        $options = $this->insertOptions;

        if ($options === null) {
            return $query;
        }

        if ($options->skipConflicts) {
            return $query->onConflictDoNothing();
        }

        $conflictTarget = null;

        if ($options->conflictColumns !== []) {
            $conflictTarget = conflict_columns($options->conflictColumns);
        } elseif ($options->conflictConstraint !== null) {
            $conflictTarget = conflict_constraint($options->conflictConstraint);
        }

        if ($conflictTarget === null) {
            return $query;
        }

        $updateColumns = $options->updateColumns !== [] ? $options->updateColumns : null;

        return $query->onConflictDoUpdate($conflictTarget, $updateColumns);
    }

    /**
     * @param list<string> $primaryKeys
     *
     * @return list<Comparison>
     */
    private function buildWhereConditions(Row $row, array $primaryKeys) : array
    {
        $rowArray = $row->toArray();
        $conditions = [];

        foreach ($primaryKeys as $key) {
            if (!\array_key_exists($key, $rowArray)) {
                throw new RuntimeException(\sprintf('Primary key "%s" not found in row', $key));
            }

            $conditions[] = eq(
                col($key),
                $this->valueToExpression($rowArray[$key])
            );
        }

        return $conditions;
    }

    /**
     * @param Entry<mixed> $entry
     */
    private function convertEntryValue(Entry $entry) : mixed
    {
        if ($entry instanceof XMLEntry || $entry instanceof XMLElementEntry) {
            return $entry->toString();
        }

        $value = $entry->value();

        if ($value === null) {
            return null;
        }

        if (\is_array($value)) {
            return \json_encode($value, \JSON_THROW_ON_ERROR);
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format('Y-m-d H:i:s.uP');
        }

        if ($value instanceof \Stringable) {
            return (string) $value;
        }

        return $value;
    }

    private function deleteRows(Rows $rows) : void
    {
        if ($this->deleteOptions === null) {
            throw new RuntimeException('DeleteOptions must be set for DELETE operation');
        }

        $primaryKeys = $this->deleteOptions->primaryKeys;

        if ($primaryKeys === []) {
            throw new RuntimeException('Primary keys must be specified for DELETE operation');
        }

        foreach ($rows as $row) {
            $whereConditions = $this->buildWhereConditions($row, $primaryKeys);

            $query = delete()->from($this->table)->where(cond_and(...$whereConditions));

            $this->client->execute($query);
        }
    }

    private function insertRows(Rows $rows) : void
    {
        $sortedRows = $rows->sortEntries();
        $firstRow = $sortedRows->first();
        $columns = [];

        foreach ($firstRow->entries() as $entry) {
            $columns[] = $entry->name();
        }

        $allParams = [];

        foreach ($sortedRows as $row) {
            foreach ($row->entries() as $entry) {
                $allParams[] = $this->convertEntryValue($entry);
            }
        }

        $query = bulk_insert($this->table, $columns, $sortedRows->count());

        if ($this->insertOptions !== null && $this->insertOptions->hasConflictHandling()) {
            $query = $this->applyBulkConflictHandling($query);
        }

        $this->client->execute($query, $allParams);
    }

    private function updateRows(Rows $rows) : void
    {
        if ($this->updateOptions === null) {
            throw new RuntimeException('UpdateOptions must be set for UPDATE operation');
        }

        $primaryKeys = $this->updateOptions->primaryKeys;

        if ($primaryKeys === []) {
            throw new RuntimeException('Primary keys must be specified for UPDATE operation');
        }

        foreach ($rows as $row) {
            $rowArray = $row->toArray();

            $assignments = [];

            foreach ($rowArray as $column => $value) {
                if (\in_array($column, $primaryKeys, true)) {
                    continue;
                }

                $assignments[$column] = $this->valueToExpression($value);
            }

            if ($assignments === []) {
                continue;
            }

            $whereConditions = $this->buildWhereConditions($row, $primaryKeys);

            $query = update()
                ->update($this->table)
                ->setAll($assignments)
                ->where(cond_and(...$whereConditions));

            $this->client->execute($query);
        }
    }

    private function valueToExpression(mixed $value) : Expression
    {
        if ($value === null) {
            return literal(null);
        }

        if (\is_string($value)) {
            return literal($value);
        }

        if (\is_int($value)) {
            return literal($value);
        }

        if (\is_float($value)) {
            return literal($value);
        }

        if (\is_bool($value)) {
            return literal($value);
        }

        if ($value instanceof \DateTimeInterface) {
            return literal($value->format('Y-m-d H:i:s.u'));
        }

        if (\is_array($value)) {
            return literal(\json_encode($value, \JSON_THROW_ON_ERROR));
        }

        if ($value instanceof \Stringable) {
            return literal((string) $value);
        }

        throw new RuntimeException(\sprintf('Cannot convert value of type %s to expression', \get_debug_type($value)));
    }
}
