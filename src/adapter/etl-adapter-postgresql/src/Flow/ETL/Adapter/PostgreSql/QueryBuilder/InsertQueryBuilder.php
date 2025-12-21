<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\QueryBuilder;

use function Flow\PostgreSql\DSL\{bulk_insert, conflict_columns, conflict_constraint};
use Flow\ETL\Adapter\PostgreSql\{EntryTypesMap, LoaderOptions\InsertOptions};
use Flow\ETL\{Row\Entry, Rows};
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\QueryBuilder\Insert\BulkInsert;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

final readonly class InsertQueryBuilder
{
    public function __construct(
        private string $table,
        private EntryTypesMap $typesMap,
    ) {
    }

    /**
     * @return array{SqlQuery, list<null|TypedValue>}
     */
    public function build(Rows $rows, ?InsertOptions $options = null) : array
    {
        $sortedRows = $rows->sortEntries();
        $firstRow = $sortedRows->first();
        $columns = [];

        foreach ($firstRow->entries() as $entry) {
            $columns[] = $entry->name();
        }

        $params = [];

        foreach ($sortedRows as $row) {
            foreach ($row->entries() as $entry) {
                $params[] = $this->mapEntryToParameter($entry);
            }
        }

        $query = bulk_insert($this->table, $columns, $sortedRows->count());

        if ($options !== null && $options->hasConflictHandling()) {
            $query = $this->applyConflictHandling($query, $options);
        }

        return [$query, $params];
    }

    private function applyConflictHandling(BulkInsert $query, InsertOptions $options) : BulkInsert
    {
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
     * @param Entry<mixed> $entry
     */
    private function mapEntryToParameter(Entry $entry) : ?TypedValue
    {
        return $this->typesMap->mapEntry($entry);
    }
}
