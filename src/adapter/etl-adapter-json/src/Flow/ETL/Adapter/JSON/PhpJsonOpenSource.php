<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonFileReader;
use Flow\ETL\Column\Backend;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Generator;

final readonly class PhpJsonOpenSource implements JsonOpenSource
{
    public function __construct(
        private JsonFileReader $reader,
        private SourceFile $source,
    ) {}

    /**
     * @return Generator<int, Rows>
     */
    public function batches(Schema $schema, int $batchSize, Backend $backend): Generator
    {
        foreach ($this->reader->batches($this->source, $batchSize) as $raw) {
            yield (new RowsBuilder($schema, $backend))
                ->appendRows($raw)
                ->finish();
        }
    }

    /**
     * JsonFileReader::batches() opens and closes its own stream.
     */
    public function close(): void {}
}
