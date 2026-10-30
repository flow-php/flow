<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Extractor;

use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RecordBatch;
use Flow\ETL\Values\Schema;
use Generator;

final readonly class PhpJsonOpenSource implements JsonOpenSource
{
    public function __construct(
        private JsonFileReader $reader,
        private SourceFile $source,
        private ?FileSplit $window = null,
    ) {}

    /**
     * @return Generator<int, Rows>
     */
    public function batches(Schema $schema, int $batchSize, Backend $backend): Generator
    {
        $batch = new RecordBatch($schema, $backend, $batchSize);

        foreach ($this->reader->sample($this->source, window: $this->window) as $values) {
            $rows = $batch->add($values);

            if ($rows !== null) {
                yield $rows;
            }
        }

        $rest = $batch->flush();

        if ($rest !== null) {
            yield $rest;
        }
    }

    /**
     * JsonFileReader::sample() opens and closes its own stream.
     */
    public function close(): void {}
}
