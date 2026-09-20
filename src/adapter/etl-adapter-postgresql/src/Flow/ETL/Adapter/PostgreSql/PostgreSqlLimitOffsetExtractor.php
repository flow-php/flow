<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
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
use function min;

final class PostgreSqlLimitOffsetExtractor implements BatchableExtractor, Extractor, RewindableExtractor
{
    use Batches;

    private ?int $maximum = null;

    private ?Schema $derivedSchema = null;

    private ?ReadQuery $read = null;

    private ?SchemaNotDerivableException $refusal = null;

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
    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        $read = $this->read ??= ReadQuery::of($this->query, self::class);

        if (!$read->isOrdered()) {
            throw new InvalidArgumentException(
                'LIMIT/OFFSET pagination requires ORDER BY clause for deterministic results',
            );
        }

        $schema = $this->schema();
        $maximum = match (true) {
            $this->maximum !== null && $limit !== null => min($this->maximum, $limit),
            $this->maximum !== null => $this->maximum,
            default => $limit,
        };

        $total = $maximum ?? $this->client->fetchScalarInt($read->count(), $this->parameters);

        if ($total === 0) {
            return;
        }

        $encoder = new PostgreSqlEncoder();
        $yielded = 0;
        $pages = (int) ceil($total / $this->batchSize);
        $pageSql = $read->page(count($this->parameters) + 1);

        for ($page = 0; $page < $pages; $page++) {
            // the request asks only for what is still wanted, while the offset keeps striding by the batch size
            $pageSize = $maximum === null ? $this->batchSize : min($this->batchSize, $maximum - $yielded);

            $cursor = $this->client->cursor($pageSql, [...$this->parameters, $pageSize, $page * $this->batchSize]);

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
        if ($this->schema !== null) {
            return $this->schema;
        }

        if ($this->refusal !== null) {
            throw $this->refusal;
        }

        try {
            return $this->derivedSchema ??= (new ResultSchema())->of(
                $this->client,
                $this->read ??= ReadQuery::of($this->query, self::class),
                $this->parameters,
                self::class,
            );
        } catch (SchemaNotDerivableException $refusal) {
            $this->refusal = $refusal;

            throw $refusal;
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

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
