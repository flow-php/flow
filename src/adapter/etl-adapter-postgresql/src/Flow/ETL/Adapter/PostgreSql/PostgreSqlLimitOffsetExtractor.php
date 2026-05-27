<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\Sql;
use Generator;

use function ceil;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\PostgreSql\DSL\sql_parse;
use function Flow\PostgreSql\DSL\sql_query_order_by;
use function Flow\PostgreSql\DSL\sql_to_count_query;
use function Flow\PostgreSql\DSL\sql_to_paginated_query;

final class PostgreSqlLimitOffsetExtractor implements Extractor
{
    private ?int $maximum = null;

    private int $pageSize = 1000;

    private ?Schema $schema = null;

    /**
     * @param list<mixed> $parameters
     */
    public function __construct(
        private readonly Client $client,
        private readonly string|Sql $query,
        private readonly array $parameters = [],
    ) {}

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $sql = $this->query instanceof Sql ? $this->query->toSql() : $this->query;

        if (!sql_query_order_by(sql_parse($sql))->hasOrderBy()) {
            throw new InvalidArgumentException(
                'LIMIT/OFFSET pagination requires ORDER BY clause for deterministic results',
            );
        }

        $total = $this->maximum ?? $this->countTotal($sql);

        if ($total === 0) {
            return;
        }

        $totalFetched = 0;
        $pages = (int) ceil($total / $this->pageSize);

        for ($page = 0; $page < $pages; $page++) {
            $offset = $page * $this->pageSize;

            $paginatedSql = $this->applyPagination($sql, $this->pageSize, $offset);

            $cursor = $this->client->cursor($paginatedSql, $this->parameters);

            foreach ($cursor->iterate() as $row) {
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
        }
    }

    public function withMaximum(int $maximum): self
    {
        if ($maximum <= 0) {
            throw new InvalidArgumentException('Maximum must be greater than 0, got ' . $maximum);
        }

        $this->maximum = $maximum;

        return $this;
    }

    public function withPageSize(int $pageSize): self
    {
        if ($pageSize <= 0) {
            throw new InvalidArgumentException('Page size must be greater than 0, got ' . $pageSize);
        }

        $this->pageSize = $pageSize;

        return $this;
    }

    public function withSchema(Schema $schema): self
    {
        $this->schema = $schema;

        return $this;
    }

    private function applyPagination(string $sql, int $limit, int $offset): string
    {
        return sql_to_paginated_query($sql, $limit, $offset);
    }

    private function countTotal(string $sql): int
    {
        return $this->client->fetchScalarInt(sql_to_count_query($sql), $this->parameters);
    }
}
