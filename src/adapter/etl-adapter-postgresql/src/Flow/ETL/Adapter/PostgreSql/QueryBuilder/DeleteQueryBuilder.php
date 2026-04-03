<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\QueryBuilder;

use function Flow\PostgreSql\DSL\{and_, col, delete, eq, param};
use Flow\ETL\Adapter\PostgreSql\{EntryTypesMap, LoaderOptions\DeleteOptions};
use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\ETL\{Row, Row\Entry};
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\QueryBuilder\Sql;

final readonly class DeleteQueryBuilder
{
    public function __construct(
        private string $table,
        private EntryTypesMap $typesMap,
    ) {
    }

    /**
     * @return array{Sql, list<null|TypedValue>}
     */
    public function build(Row $row, DeleteOptions $options) : array
    {
        $primaryKeys = $options->primaryKeys;

        if ($primaryKeys === []) {
            throw new RuntimeException('Primary keys must be specified for DELETE operation');
        }

        $paramIndex = 1;
        $params = [];
        $conditions = [];

        foreach ($primaryKeys as $key) {
            if (!$row->has($key)) {
                throw new RuntimeException(\sprintf('Primary key "%s" not found in row', $key));
            }

            $entry = $row->get($key);

            $conditions[] = eq(
                col($key),
                param($paramIndex++)
            );
            $params[] = $this->mapEntryToParameter($entry);
        }

        $query = delete()->from($this->table)->where(and_(...$conditions));

        return [$query, $params];
    }

    /**
     * @param Entry<mixed> $entry
     */
    private function mapEntryToParameter(Entry $entry) : ?TypedValue
    {
        return $this->typesMap->mapEntry($entry);
    }
}
