<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\QueryBuilder;

use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\InsertOptions;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\QueryBuilder\Insert\BulkInsert;
use Flow\PostgreSql\QueryBuilder\Sql;

use function Flow\PostgreSql\DSL\bulk_insert;
use function Flow\PostgreSql\DSL\conflict_columns;
use function Flow\PostgreSql\DSL\conflict_constraint;

final readonly class InsertQueryBuilder
{
    public function __construct(
        private string $table,
        private EntryTypesMap $typesMap,
    ) {}

    /**
     * @return array{Sql, list<null|TypedValue>}
     */
    public function build(Rows $rows, ?InsertOptions $options = null): array
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

    private function applyConflictHandling(BulkInsert $query, InsertOptions $options): BulkInsert
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
    private function mapEntryToParameter(Entry $entry): ?TypedValue
    {
        return $this->typesMap->mapEntry($entry);
    }
}
