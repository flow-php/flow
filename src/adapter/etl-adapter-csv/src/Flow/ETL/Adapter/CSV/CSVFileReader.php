<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Inference\SchemaSampler;
use Generator;

use function count;

final readonly class CSVFileReader implements SchemaSampler
{
    /**
     * @param list<SourceFile> $sources materialised, not a Generator: header() and samples() both walk it
     */
    public function __construct(
        private CSVSourceOpener $opener,
        private array $sources,
    ) {}

    /**
     * Abandoning the generator closes the source.
     *
     * @param int<1, max> $batchSize
     *
     * @return Generator<int, non-empty-list<RawRowValues>>
     */
    public function batches(SourceFile $source, int $batchSize): Generator
    {
        $open = $this->opener->open($source);
        $batch = [];

        try {
            foreach ($open->records() as $values) {
                $batch[] = $values;

                if (count($batch) >= $batchSize) {
                    yield $batch;
                    $batch = [];
                }
            }

            if ($batch !== []) {
                yield $batch;
            }
        } finally {
            $open->close();
        }
    }

    /**
     * @return list<string>
     */
    public function columns(SourceFile $source): array
    {
        $open = $this->opener->open($source);

        try {
            return $open->columns();
        } finally {
            $open->close();
        }
    }

    public function header(): CSVHeader
    {
        foreach ($this->sources as $source) {
            $columns = $this->columns($source);

            if ($columns !== []) {
                return new CSVHeader($columns, $source->uri());
            }
        }

        return new CSVHeader([], null);
    }

    /**
     * @return Generator<int, RawRowValues>
     */
    public function sample(SourceFile $source): Generator
    {
        $open = $this->opener->open($source);

        try {
            yield from $open->records();
        } finally {
            $open->close();
        }
    }

    /**
     * $rowBudget is deliberately unused: sample() is lazy and SchemaInferrer stops advancing it.
     *
     * @return Generator<int, Generator<int, RawRowValues>>
     */
    public function samples(int $rowBudget): iterable
    {
        foreach ($this->sources as $source) {
            yield $this->sample($source);
        }
    }
}
