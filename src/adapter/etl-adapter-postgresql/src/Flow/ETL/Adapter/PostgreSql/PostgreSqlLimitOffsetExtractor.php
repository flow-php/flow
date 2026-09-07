<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\Sql;
use Generator;

use function ceil;
use function Flow\PostgreSql\DSL\sql_parse;
use function Flow\PostgreSql\DSL\sql_query_order_by;
use function Flow\PostgreSql\DSL\sql_to_count_query;
use function Flow\PostgreSql\DSL\sql_to_paginated_query;

final class PostgreSqlLimitOffsetExtractor implements Extractor, RewindableExtractor
{
    private ?int $maximum = null;

    private int $pageSize = 1000;

    private ?Schema $derivedSchema = null;

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

        $schema = $this->schema();

        $total = $this->maximum ?? $this->countTotal($sql);

        if ($total === 0) {
            return;
        }

        $encoder = new PostgreSqlEncoder();
        $totalFetched = 0;
        $pages = (int) ceil($total / $this->pageSize);

        for ($page = 0; $page < $pages; $page++) {
            $offset = $page * $this->pageSize;

            $paginatedSql = $this->applyPagination($sql, $this->pageSize, $offset);

            $cursor = $this->client->cursor($paginatedSql, $this->parameters);

            $rawBatch = [];

            foreach ($cursor->iterate() as $row) {
                $rawBatch[] = $row;
            }

            $cursor->free();

            $hydrated = $context->hydrator()->hydrate($encoder->decode($rawBatch), $schema);

            foreach ($hydrated as $hydratedRow) {
                $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

                $totalFetched++;

                if ($signal === Signal::STOP) {
                    return;
                }

                if ($this->maximum !== null && $totalFetched >= $this->maximum) {
                    return;
                }
            }
        }
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    public function schema(): Schema
    {
        return (
            $this->schema ?? ($this->derivedSchema ??= (new ResultSchema())->of(
                $this->client,
                $this->query,
                $this->parameters,
                self::class,
            ))
        );
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

    public function withSchema(Schema $schema): static
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
