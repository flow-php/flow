<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Exception\RuntimeException as FlowRuntimeException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class NativeJsonReader
{
    /**
     * $uri names the file in refusals.
     */
    public function __construct(bool $lines, string $uri)
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function feed(string $chunk): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * Malformed input left in the buffer (a truncated record, a missing `]`) is refused by the next nextColumns().
     */
    public function finish(): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * The next batch of exactly $batchSize rows, the last one shorter after finish(); null while no full batch is
     * buffered, or when nothing is left after finish().
     *
     * @param int<1, max> $batchSize
     *
     * @throws SchemaMismatchException a cast or schema refusal, as RowsBuilder::appendRows() throws it
     * @throws FlowRuntimeException malformed JSON, or a scalar record
     */
    public function nextColumns(Schema $schema, int $batchSize): ?Rows
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
