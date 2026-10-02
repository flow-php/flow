<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Column\Backend;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaSampler;
use Flow\Filesystem\Filesystem;
use Generator;

final readonly class CSVFileReader implements SchemaSampler
{
    /**
     * @param list<SourceFile> $sources materialised, not a Generator: header() and samples() both walk it
     */
    public function __construct(
        private Filesystem $filesystem,
        private CSVReadOptions $options,
        private array $sources,
    ) {}

    /**
     * Abandoning the generator closes the source.
     *
     * @param int<1, max> $batchSize
     *
     * @return Generator<int, Rows>
     */
    public function batches(SourceFile $source, Schema $schema, int $batchSize, Backend $backend): Generator
    {
        $open = new AdaptiveCSVOpenSource($this->filesystem, $source, $this->options);

        try {
            yield from $open->batches($schema, $batchSize, $backend);
        } finally {
            $open->close();
        }
    }

    /**
     * @return list<string>
     */
    public function columns(SourceFile $source): array
    {
        $open = new AdaptiveCSVOpenSource($this->filesystem, $source, $this->options);

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
     * $rowBudget is unused: SchemaInferrer hands each unit its remaining budget through sniffColumnTypes().
     *
     * @return Generator<int, CSVFileSample>
     */
    public function samples(int $rowBudget): iterable
    {
        foreach ($this->sources as $source) {
            yield new CSVFileSample($this->filesystem, $this->options, $source);
        }
    }
}
