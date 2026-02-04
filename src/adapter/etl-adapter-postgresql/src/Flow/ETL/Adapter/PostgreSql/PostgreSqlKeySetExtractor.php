<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\PostgreSql\DSL\sql_to_keyset_query;
use Flow\ETL\Adapter\PostgreSql\Pagination\{Key, KeySet};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\Extractor\Signal;
use Flow\ETL\{Extractor, FlowContext, Schema};
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

final class PostgreSqlKeySetExtractor implements Extractor
{
    private ?int $maximum = null;

    private int $pageSize = 1000;

    private ?Schema $schema = null;

    /**
     * @param array<int, mixed> $parameters
     */
    public function __construct(
        private readonly Client $client,
        private readonly string|SqlQuery $query,
        private readonly KeySet $keySet,
        private readonly array $parameters = [],
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $uri = 'postgresql://keyset';
        $sql = $this->query instanceof SqlQuery ? $this->query->toSql() : $this->query;

        $totalFetched = 0;
        $cursorValues = null;

        while (true) {
            $paginatedSql = $this->applyKeysetPagination($sql, $this->pageSize, $cursorValues);

            $cursor = $this->client->cursor($paginatedSql, \array_merge($this->parameters, $cursorValues ?? []));

            $hasRows = false;
            $lastRow = null;

            foreach ($cursor->iterate() as $row) {
                $hasRows = true;
                $lastRow = $row;

                $signal = yield array_to_rows($row, $context->entryFactory(), [], $this->schema);

                $totalFetched++;

                if ($signal === Signal::STOP) {
                    $cursor->free();

                    return;
                }

                if ($this->maximum !== null && $totalFetched >= $this->maximum) {
                    $cursor->free();

                    return;
                }
            }

            $cursor->free();

            if (!$hasRows) {
                break;
            }

            $cursorValues = $this->extractCursorValues($lastRow);
        }
    }

    public function withMaximum(int $maximum) : self
    {
        if ($maximum <= 0) {
            throw new InvalidArgumentException('Maximum must be greater than 0, got ' . $maximum);
        }

        $this->maximum = $maximum;

        return $this;
    }

    public function withPageSize(int $pageSize) : self
    {
        if ($pageSize <= 0) {
            throw new InvalidArgumentException('Page size must be greater than 0, got ' . $pageSize);
        }

        $this->pageSize = $pageSize;

        return $this;
    }

    public function withSchema(Schema $schema) : self
    {
        $this->schema = $schema;

        return $this;
    }

    /**
     * @param null|list<null|bool|float|int|string> $cursorValues
     */
    private function applyKeysetPagination(string $sql, int $limit, ?array $cursorValues) : string
    {
        return sql_to_keyset_query($sql, $limit, $this->keySet->toKeysetColumns(), $cursorValues);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return list<bool|float|int|string>
     */
    private function extractCursorValues(array $row) : array
    {
        $values = [];

        foreach ($this->keySet->keys as $key) {
            $columnName = $this->getColumnName($key);

            if (!\array_key_exists($columnName, $row)) {
                throw new RuntimeException(\sprintf(
                    'Column "%s" not found in result row for keyset pagination',
                    $columnName
                ));
            }

            $value = $row[$columnName];

            if ($value === null) {
                throw new RuntimeException(\sprintf(
                    'NULL value found in column "%s" for keyset pagination; key columns must be non-null',
                    $columnName
                ));
            }

            if (!\is_string($value) && !\is_int($value) && !\is_float($value) && !\is_bool($value)) {
                throw new RuntimeException(\sprintf(
                    'Unsupported value type "%s" in column "%s" for keyset pagination',
                    \get_debug_type($value),
                    $columnName
                ));
            }

            $values[] = $value;
        }

        return $values;
    }

    private function getColumnName(Key $key) : string
    {
        $parts = \explode('.', $key->column);

        return \end($parts);
    }
}
