<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\LimitPushDown;
use Flow\ETL\Extractor\PushesLimit;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\Sql;
use Generator;

use function ceil;
use function count;
use function Flow\PostgreSql\DSL\sql_parse;
use function Flow\PostgreSql\DSL\sql_query_order_by;
use function Flow\PostgreSql\DSL\sql_to_count_query;
use function Flow\PostgreSql\DSL\sql_to_paginated_query;
use function min;

final class PostgreSqlLimitOffsetExtractor implements BatchableExtractor, Extractor, LimitPushDown, RewindableExtractor
{
    use Batches;
    use PushesLimit;

    private ?int $maximum = null;

    private ?Schema $derivedSchema = null;

    private ?Schema $schema = null;

    /**
     * @param list<mixed> $parameters
     */
    public function __construct(
        private readonly Client $client,
        private readonly string|Sql $query,
        private readonly array $parameters = [],
    ) {
        // a page is a network round trip, not a buffer: 100 would cost 10x the round trips
        $this->batchSize = 1_000;
    }

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

        $pushed = $this->pushedLimit();
        $maximum = match (true) {
            $this->maximum !== null && $pushed !== null => min($this->maximum, $pushed),
            $this->maximum !== null => $this->maximum,
            default => $pushed,
        };

        $total = $maximum ?? $this->countTotal($sql);

        if ($total === 0) {
            return;
        }

        $encoder = new PostgreSqlEncoder();
        $yielded = 0;
        $pages = (int) ceil($total / $this->batchSize);

        for ($page = 0; $page < $pages; $page++) {
            // the request asks only for what is still wanted, while the offset keeps striding by the batch size
            $pageSize = $maximum === null ? $this->batchSize : min($this->batchSize, $maximum - $yielded);
            $offset = $page * $this->batchSize;

            $paginatedSql = $this->applyPagination($sql, $pageSize, $offset);

            $cursor = $this->client->cursor($paginatedSql, $this->parameters);

            $rawBatch = [];

            foreach ($cursor->iterate() as $row) {
                $rawBatch[] = $row;
            }

            $cursor->free();

            if ($rawBatch === []) {
                return;
            }

            $hydrated = $context->hydrator()->hydrate($encoder->decode($rawBatch), $schema);

            $yielded += $hydrated->count();

            $signal = yield $hydrated;

            if ($signal === Signal::STOP) {
                return;
            }

            // a short page means the source ran out, whatever $total promised
            if (count($rawBatch) < $pageSize) {
                return;
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
