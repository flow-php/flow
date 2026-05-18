<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\QueryBuilder;

use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\UpdateOptions;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\QueryBuilder\Sql;

use function Flow\PostgreSql\DSL\and_;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\update;
use function in_array;
use function sprintf;

final readonly class UpdateQueryBuilder
{
    public function __construct(
        private string $table,
        private EntryTypesMap $typesMap,
    ) {}

    /**
     * @return array{null|Sql, list<null|TypedValue>}
     */
    public function build(Row $row, UpdateOptions $options): array
    {
        $primaryKeys = $options->primaryKeys;

        if ($primaryKeys === []) {
            throw new RuntimeException('Primary keys must be specified for UPDATE operation');
        }

        $paramIndex = 1;
        $params = [];
        $assignments = [];

        foreach ($row->entries() as $entry) {
            if (in_array($entry->name(), $primaryKeys, true)) {
                continue;
            }

            $assignments[$entry->name()] = param($paramIndex++);
            $params[] = $this->mapEntryToParameter($entry);
        }

        if ($assignments === []) {
            return [null, []];
        }

        $conditions = [];

        foreach ($primaryKeys as $key) {
            if (!$row->has($key)) {
                throw new RuntimeException(sprintf('Primary key "%s" not found in row', $key));
            }

            $entry = $row->get($key);

            $conditions[] = eq(col($key), param($paramIndex++));
            $params[] = $this->mapEntryToParameter($entry);
        }

        $query = update()->update($this->table)->setAll($assignments)->where(and_(...$conditions));

        return [$query, $params];
    }

    /**
     * @param Entry<mixed> $entry
     */
    private function mapEntryToParameter(Entry $entry): ?TypedValue
    {
        return $this->typesMap->mapEntry($entry);
    }
}
