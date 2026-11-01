<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Extractor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Inference\SchemaInference;
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
     * @return Generator<int, CorruptedRecordException|Rows>
     */
    public function batches(
        Schema $schema,
        int $batchSize,
        Backend $backend,
        ?SchemaInference $inference = null,
        ?Schema $known = null,
    ): Generator {
        $batch = $inference === null
            ? new RecordBatch($schema, $backend, $batchSize, $this->source->uri(), $known ?? $schema)
            : RecordBatch::inferred(
                $schema,
                $backend,
                $batchSize,
                $this->source->uri(),
                $inference->sampleSize,
                $known ?? $schema,
            );
        $read = new JsonReadProgress();

        foreach ($this->reader->sample($this->source, $read, $this->window) as $values) {
            yield from $batch->add($values, $read->lastOffset(), $read->lastLine(), $read->lastRecord());
        }

        yield from $batch->flush();
    }

    /**
     * JsonFileReader::sample() opens and closes its own stream.
     */
    public function close(): void {}
}
