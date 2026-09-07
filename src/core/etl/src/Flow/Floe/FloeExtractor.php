<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\FileReading;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Hydrator;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Codec\NoopCodec;
use Generator;

use function sprintf;

final class FloeExtractor implements
    Extractor,
    FileExtractor,
    LimitableExtractor,
    MetadataColumnsExtractor,
    RewindableExtractor
{
    private ?Schema $schema = null;

    use Limitable;
    use FileReading;

    private ?int $offset = null;

    private bool $unionByName = false;

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

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return \Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $fileOffset = $this->offset ?? 0;
        // schema() opens a footer, so it is asked only when a schema was declared - an undeclared
        // read is gated batch by batch by the checked door below instead
        $promisedSchema = $this->schema === null ? null : $this->schema();

        $fileColumns = $this->fileColumns($this->filesystem, $this->path);

        foreach ($this->files($context->hydrator()) as $file) {
            // finally, not a close() per exit: the offset-skip continue, the STOP/limit return
            // below and an abandoned generator all have to release the handle (b73)
            try {
                $fileRows = $file->reader->totalRows();

                if ($fileOffset >= $fileRows) {
                    $fileOffset -= $fileRows;

                    continue;
                }

                // R6: over the FILE's schema, never over schema()'s output
                $fileSchema = $fileColumns->declare($this->schema ?? $file->schema());
                $constants = $fileColumns->forFile($file->source(), $fileSchema);

                $limit = $this->limit();
                $remaining = $limit === null ? null : $limit - $this->yieldedRows;

                foreach ($file->reader->rows(1000, $fileOffset, $remaining) as $rows) {
                    // R7: the stamp stays post-hydration - FloeStreamReader::rows() yields hydrated Rows and
                    // must not learn about paths - but the constants are the shared ones, already typed
                    $filled = [];

                    foreach ($rows->all() as $row) {
                        $filled[] = new Row($constants->fill($row->values()));
                    }

                    // the reader already matched every row against the footer schema, and the tail is
                    // written in the order declare() emits it, so a second full check buys nothing
                    $rows = Rows::trusted($fileSchema, $filled);

                    if ($promisedSchema !== null) {
                        $rows = $rows->matchTo($promisedSchema);
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
            } finally {
                $file->close();
            }
        }
    }

    /**
     * Footer-only source schema (two ranged reads per file, no row scan). One file unless
     * unionByName() asks for the fold, and memoised, so repeated calls cost nothing.
     */
    public function schema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->declare(
            $this->schema ?? $this->derivedSchema($this->files(), $this->unionByName),
        );
    }

    /**
     * Reconcile every listed file's footer instead of trusting the first one.
     */
    public function unionByName(bool $union = true): self
    {
        $this->unionByName = $union;
        $this->derivedSchema = null;

        return $this;
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
     * @return Generator<int, FloeSourceFile>
     */
    private function files(?Hydrator $hydrator = null): Generator
    {
        foreach ($this->sourceFiles($this->filesystem, $this->path) as $source) {
            yield new FloeSourceFile(
                (new FloeReader(
                    $this->filesystem,
                    $this->codec,
                    $this->chunkSize,
                    hydrator: $hydrator,
                    engine: $this->engine,
                ))->read($source->path),
                $source,
            );
        }
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
