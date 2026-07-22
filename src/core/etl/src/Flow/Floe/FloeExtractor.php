<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\PathFiltering;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Schema;
use Flow\Filesystem\Path;
use Flow\Floe\Codec\NoopCodec;
use Generator;

use function Flow\ETL\DSL\str_entry;

final class FloeExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
    use PathFiltering;

    private ?int $offset = null;

    public function __construct(
        private readonly Path $path,
        private readonly Codec $codec = new NoopCodec(),
        private readonly int $chunkSize = 65536,
        private readonly FloeEngine $engine = FloeEngine::adaptive,
    ) {
        $this->resetLimit();
    }

    /**
     * @return \Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $putInputIntoRows = $context->config->shouldPutInputIntoRows();
        $fileOffset = $this->offset ?? 0;

        foreach ($this->readers($context) as [$reader, $uri]) {
            $fileRows = $reader->totalRows();

            if ($fileOffset >= $fileRows) {
                $fileOffset -= $fileRows;

                continue;
            }

            $limit = $this->limit();
            $remaining = $limit === null ? null : $limit - $this->yieldedRows;

            foreach ($reader->rows(1000, $fileOffset, $remaining) as $rows) {
                if ($putInputIntoRows) {
                    $rows = $rows->map(static fn(Row $row): Row => $row->add(str_entry('_input_file_uri', $uri)));
                }

                $signal = yield $rows;

                foreach ($rows as $row) {
                    $this->incrementReturnedRows();
                }

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    return;
                }
            }

            $fileOffset = 0;
        }
    }

    /**
     * Footer-only source schema (two ranged reads per file, no row scan).
     */
    public function schema(FlowContext $context): Schema
    {
        $schema = new Schema();

        foreach ($this->readers($context) as [$reader]) {
            $schema = $schema->merge($reader->schema());
        }

        return $schema;
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function withOffset(int $offset): self
    {
        if ($offset < 0) {
            throw new InvalidArgumentException('Offset must be greater or equal to 0');
        }

        $this->offset = $offset;

        return $this;
    }

    /**
     * @return \Generator<int, array{FloeStreamReader, string}>
     */
    private function readers(FlowContext $context): Generator
    {
        foreach ($context->streams()->list($this->path, $this->filter()) as $listed) {
            $filePath = $listed->path();
            $listed->close();

            yield [
                (new FloeReader(
                    $context->filesystem($this->path),
                    $this->codec,
                    $this->chunkSize,
                    hydrator: $context->hydrator(),
                    engine: $this->engine,
                ))->read($filePath),
                $filePath->uri(),
            ];
        }
    }
}
