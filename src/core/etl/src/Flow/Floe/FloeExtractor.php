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
use Flow\ETL\Row\Hydrator;
use Flow\ETL\Schema;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Codec\NoopCodec;
use Generator;

use function Flow\ETL\DSL\str_entry;
use function sprintf;

final class FloeExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
    use PathFiltering;

    private ?int $offset = null;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        private readonly Codec $codec = new NoopCodec(),
        private readonly int $chunkSize = 65536,
        private readonly FloeEngine $engine = FloeEngine::adaptive,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_floe($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
        $this->resetLimit();
    }

    /**
     * @return \Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $putInputIntoRows = $context->config->shouldPutInputIntoRows();
        $fileOffset = $this->offset ?? 0;

        foreach ($this->readers($context->hydrator()) as [$reader, $uri]) {
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
    public function schema(): Schema
    {
        $schema = new Schema();

        foreach ($this->readers() as [$reader]) {
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
    private function readers(?Hydrator $hydrator = null): Generator
    {
        foreach ((new FileListing($this->filesystem))->list($this->path, $this->filter()) as $listedFile) {
            yield [
                (new FloeReader(
                    $this->filesystem,
                    $this->codec,
                    $this->chunkSize,
                    hydrator: $hydrator,
                    engine: $this->engine,
                ))->read($listedFile->path),
                $listedFile->path->uri(),
            ];
        }
    }
}
