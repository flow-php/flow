<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\Pagination\Key;
use Flow\ETL\Adapter\PostgreSql\Pagination\KeySet;
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
use Flow\ETL\Schema;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\Sql;
use Generator;

use function array_key_exists;
use function count;
use function end;
use function explode;
use function get_debug_type;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
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

        $encoder = new PostgreSqlEncoder();
        $yielded = 0;
        $cursorValues = null;
        $maximum = match (true) {
            $this->maximum !== null && $limit !== null => min($this->maximum, $limit),
            $this->maximum !== null => $this->maximum,
            default => $limit,
        };
        $first = count($this->parameters) + 1;
        $firstPage = $read->keySetFirstPage($this->keySet, $first);
        $nextPage = null;

        while (true) {
            if ($maximum !== null && $yielded >= $maximum) {
                return;
            }

            $cursor = $this->client->cursor(
                $cursorValues === null ? $firstPage : ($nextPage ??= $read->keySetNextPage($this->keySet, $first)),
                [
                    ...$this->parameters,
                    $maximum === null ? $this->batchSize : min($this->batchSize, $maximum - $yielded),
                    ...($cursorValues ?? []),
                ],
            );

            $hasRows = false;
            $lastRow = null;
            $rawBatch = [];

            foreach ($cursor->iterate() as $row) {
                $hasRows = true;
                $lastRow = $row;
                $rawBatch[] = $row;
            }

            $cursor->free();

            if (!$hasRows || $lastRow === null) {
                break;
            }

            $hydrated = $context->hydrator()->hydrate($encoder->decode($rawBatch), $schema);

            $yielded += $hydrated->count();

            $signal = yield $hydrated;

            if ($signal === Signal::STOP) {
                return;
            }

            $cursorValues = $this->extractCursorValues($lastRow);
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

    /**
     * @param array<string, mixed> $row
     *
     * @return list<bool|float|int|string>
     */
    private function extractCursorValues(array $row): array
    {
        $values = [];

        foreach ($this->keySet->keys as $key) {
            $columnName = $this->getColumnName($key);

            if (!array_key_exists($columnName, $row)) {
                throw new RuntimeException(sprintf(
                    'Column "%s" not found in result row for keyset pagination',
                    $columnName,
                ));
            }

            // @mago-expect analysis:mixed-assignment
            $value = $row[$columnName];

            if ($value === null) {
                throw new RuntimeException(sprintf(
                    'NULL value found in column "%s" for keyset pagination; key columns must be non-null',
                    $columnName,
                ));
            }

            if (!is_string($value) && !is_int($value) && !is_float($value) && !is_bool($value)) {
                throw new RuntimeException(sprintf(
                    'Unsupported value type "%s" in column "%s" for keyset pagination',
                    get_debug_type($value),
                    $columnName,
                ));
            }

            $values[] = $value;
        }

        return $values;
    }

    private function getColumnName(Key $key): string
    {
        $parts = explode('.', $key->column);

        return end($parts);
    }
}
