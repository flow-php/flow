<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustCSVReaderNative
{
    public function __construct(
        string $separator,
        string $enclosure,
        string $escape,
        bool $withHeader,
        bool $emptyToNull,
        bool $removeBOM,
    ) {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * The bytes, as read, of every row next(), nextColumns() and fold() produced so far - line endings included, the header
     * record excluded.
     *
     * @return int<0, max>
     */
    public function consumedBytes(): int
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function feed(string $chunk): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function finish(): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param int<0, max>|-1 $limit
     *
     * @return int<0, max>
     */
    public function fold(RustColumnFoldNative $fold, int $limit): int
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @return list<string>
     */
    public function headers(): array
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param int<1, max> $batchSize
     *
     * @return list<array<array-key, ?string>>
     */
    public function next(int $batchSize): array
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * Exactly $batchSize rows keyed and ordered by $schema while that many are buffered, the remainder after finish(),
     * else null; row indexes in refusals are relative to the batch.
     *
     * @param int<1, max> $batchSize
     *
     * @throws SchemaMismatchException
     */
    public function nextColumns(Schema $schema, int $batchSize): ?Rows
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
