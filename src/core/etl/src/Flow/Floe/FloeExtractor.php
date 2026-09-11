<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\FileReading;
use Flow\ETL\Extractor\LimitPushDown;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\PushesLimit;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Hydrator;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Validator\StrictValidator;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Codec\NoopCodec;
use Generator;

use function sprintf;

final class FloeExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    LimitPushDown,
    MetadataColumnsExtractor,
    RewindableExtractor
{
    private ?Schema $schema = null;

    use Batches;
    use PushesLimit;
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
        $yielded = 0;
        $promisedSchema = $this->schema === null ? null : $this->schema();
        // undeclared, every file is read under the first file's schema, or the union of all of them
        $expected = $this->schema === null ? $this->derivedSchema($this->files(), $this->unionByName) : null;
        $target = $promisedSchema ?? $this->schema();

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

                if ($expected !== null && !$this->unionByName) {
                    $validation = (new StrictValidator())->validate($expected, $file->schema());

                    if (!$validation->isValid()) {
                        throw InferredSchemaException::filesDiverge(
                            $file->source()->uri(),
                            $this->derivedFrom,
                            $validation,
                        );
                    }
                }

                // R6: over the FILE's schema, never over schema()'s output
                $fileSchema = $fileColumns->declare($this->schema ?? $file->schema());
                $constants = $fileColumns->forFile($file->source(), $fileSchema);
                // a declared schema is always matched: the rows below are only trusted against the footer
                $matchTo = $promisedSchema ?? (!$fileSchema->isSame($target) ? $target : null);

                $limit = $this->pushedLimit();
                $remaining = $limit === null ? null : $limit - $yielded;

                foreach ($file->reader->rows($this->batchSize(), $fileOffset, $remaining) as $rows) {
                    // R7: the stamp stays post-hydration - FloeStreamReader::rows() yields hydrated Rows and
                    // must not learn about paths - but the constants are the shared ones, already typed
                    $filled = [];

                    foreach ($rows->all() as $row) {
                        $filled[] = new Row($constants->fill($row->values()));
                    }

                    // the reader already matched every row against the footer schema, and the tail is
                    // written in the order declare() emits it, so a second full check buys nothing
                    $rows = Rows::trusted($fileSchema, $filled);

                    if ($matchTo !== null) {
                        $rows = $rows->matchTo($matchTo);
                    }

                    $yielded += $rows->count();

                    $signal = yield $rows;

                    if ($signal === Signal::STOP) {
                        return;
                    }

                    if ($limit !== null && $yielded >= $limit) {
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
