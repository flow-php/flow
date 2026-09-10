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

use function bin2hex;
use function Flow\PostgreSql\DSL\close_cursor;
use function Flow\PostgreSql\DSL\declare_cursor;
use function Flow\PostgreSql\DSL\fetch;
use function min;
use function random_bytes;

/**
 * PostgreSQL extractor using server-side cursors for memory-efficient extraction.
 *
 * Uses DECLARE CURSOR + FETCH to stream data from PostgreSQL without loading
 * the entire result set into memory. This is the only way to achieve true
 * low memory extraction with PHP's ext-pgsql.
 *
 * Note: Requires a transaction context (auto-started if not in one).
 */
final class PostgreSqlCursorExtractor implements BatchableExtractor, Extractor, LimitPushDown, RewindableExtractor
{
    use Batches;
    use PushesLimit;

    private ?string $cursorName = null;

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
        // a fetch is a network round trip, not a buffer: 100 would cost 10x the round trips
        $this->batchSize = 1_000;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $encoder = new PostgreSqlEncoder();
        $cursorName = $this->cursorName ?? 'flow_cursor_' . bin2hex(random_bytes(8));

        $schema = $this->schema();

        $ownTransaction = $this->client->getTransactionNestingLevel() === 0;

        if ($ownTransaction) {
            $this->client->beginTransaction();
        }

        try {
            $this->client->execute(declare_cursor($cursorName, $this->query), $this->parameters);

            $pushed = $this->pushedLimit();
            $maximum = match (true) {
                $this->maximum !== null && $pushed !== null => min($this->maximum, $pushed),
                $this->maximum !== null => $this->maximum,
                default => $pushed,
            };
            $yielded = 0;

            while (true) {
                if ($maximum !== null && $yielded >= $maximum) {
                    return;
                }

                $pageSize = $maximum === null ? $this->batchSize : min($this->batchSize, $maximum - $yielded);
                $cursor = $this->client->cursor(fetch($cursorName)->forward($pageSize));
                $rowCount = $cursor->count();

                if ($rowCount === 0) {
                    $cursor->free();

                    break;
                }

                $rawBatch = [];

                foreach ($cursor->iterate() as $row) {
                    $rawBatch[] = $row;
                }

                $cursor->free();

                $hydrated = $context->hydrator()->hydrate($encoder->decode($rawBatch), $schema);

                $yielded += $hydrated->count();

                $signal = yield $hydrated;

                if ($signal === Signal::STOP) {
                    return;
                }

                // compared against what was asked for, so a narrowed final fetch is not read as exhausted
                if ($rowCount < $pageSize) {
                    break;
                }
            }
        } finally {
            $this->client->execute(close_cursor($cursorName));

            if ($ownTransaction) {
                $this->client->commit();
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

    public function withCursorName(string $cursorName): self
    {
        $this->cursorName = $cursorName;

        return $this;
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
