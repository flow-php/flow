<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
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
final class PostgreSqlCursorExtractor implements Extractor
{
    private ?string $cursorName = null;

    private int $fetchSize = 1000;

    private ?int $maximum = null;

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
        $encoder = new PostgreSqlEncoder();
        $cursorName = $this->cursorName ?? 'flow_cursor_' . bin2hex(random_bytes(8));

        $ownTransaction = $this->client->getTransactionNestingLevel() === 0;

        if ($ownTransaction) {
            $this->client->beginTransaction();
        }

        try {
            $this->client->execute(declare_cursor($cursorName, $this->query), $this->parameters);

            $totalFetched = 0;

            while (true) {
                $cursor = $this->client->cursor(fetch($cursorName)->forward($this->fetchSize));
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

                $hydrated = $context->hydrator()->cast($encoder->decode($rawBatch), $this->schema);

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

                if ($rowCount < $this->fetchSize) {
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

    public function schema(): Schema
    {
        if ($this->schema === null) {
            throw SchemaNotDerivableException::extractor(self::class);
        }

        return $this->schema;
    }

    public function withCursorName(string $cursorName): self
    {
        $this->cursorName = $cursorName;

        return $this;
    }

    public function withFetchSize(int $fetchSize): self
    {
        if ($fetchSize <= 0) {
            throw new InvalidArgumentException('Fetch size must be greater than 0, got ' . $fetchSize);
        }

        $this->fetchSize = $fetchSize;

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
