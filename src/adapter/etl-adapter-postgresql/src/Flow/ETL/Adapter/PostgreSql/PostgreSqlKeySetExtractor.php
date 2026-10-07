<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\Pagination\Key;
use Flow\ETL\Adapter\PostgreSql\Pagination\KeySet;
use Flow\ETL\Adapter\PostgreSql\Pagination\KeySetPage;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\Sql;
use Generator;

use function array_map;
use function count;
use function implode;
use function min;
use function sprintf;

final class PostgreSqlKeySetExtractor implements BatchableExtractor, Extractor, RewindableExtractor
{
    use Batches;

    private ?int $maximum = null;

    private ?Schema $derivedSchema = null;

    private ?ReadQuery $read = null;

    private ?SchemaNotDerivableException $refusal = null;

    private ?Schema $schema = null;

    private ?Statistics $statistics = null;

    /**
     * @param list<mixed> $parameters
     */
    public function __construct(
        private readonly Client $client,
        private readonly string|Sql $query,
        private readonly KeySet $keySet,
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

        $schema = $this->schema();

        $yielded = 0;
        $maximum = match (true) {
            $this->maximum !== null && $limit !== null => min($this->maximum, $limit),
            $this->maximum !== null => $this->maximum,
            default => $limit,
        };
        $first = count($this->parameters) + 1;
        $firstPage = $read->keySetFirstPage($this->keySet, $first);

        if ($this->client->fetchOne($read->keySetNullCheck($this->keySet), $this->parameters) !== null) {
            throw new RuntimeException(sprintf(
                'Keyset pagination requires non-null keys, but a row has NULL in the key column(s) %s; filter them out with IS NOT NULL or choose non-null keys',
                implode(', ', array_map(static fn(Key $key): string => '"' . $key->column . '"', $this->keySet->keys)),
            ));
        }

        $cursor = null;
        $expected = null;
        $nextPage = null;

        while (true) {
            if ($maximum !== null && $yielded >= $maximum) {
                return;
            }

            $size = $maximum === null ? $this->batchSize : min($this->batchSize, $maximum - $yielded);

            $pgCursor = $this->client->cursor(
                $cursor === null ? $firstPage : ($nextPage ??= $read->keySetNextPage($this->keySet, $first)),
                [...$this->parameters, $size + 1, ...($cursor->values ?? [])],
            );

            $fetched = [...$pgCursor->iterate()];

            $pgCursor->free();

            $page = KeySetPage::of($this->keySet, $fetched, $size, $expected);

            if ($page->rows !== []) {
                $rows = (new RowsBuilder($schema, $context->backend()))
                    ->appendRows($page->rows)
                    ->finish();

                $yielded += $rows->count();

                $signal = yield $rows;

                if ($signal === Signal::STOP) {
                    return;
                }
            }

            if ($page->isLast()) {
                return;
            }

            $cursor = $page->cursor;
            $expected = $page->lookahead;
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

    public function statistics(): Statistics
    {
        return $this->statistics ??= new Statistics(rows: (new ExplainedRows())->of(
            $this->client,
            $this->query,
            $this->parameters,
            $this->maximum,
        ));
    }

    public function withMaximum(int $maximum): self
    {
        if ($maximum <= 0) {
            throw new InvalidArgumentException('Maximum must be greater than 0, got ' . $maximum);
        }

        $this->maximum = $maximum;
        $this->statistics = null;

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
