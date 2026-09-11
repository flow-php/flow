<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

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
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Validator\StrictValidator;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\Reader;
use Generator;

use function array_values;
use function count;
use function max;
use function sprintf;

final class ParquetExtractor implements
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

    private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN;

    private bool $unionByName = false;

    /**
     * @var array<string>
     */
    private array $columns = [];

    private ?ParquetEngine $engine = null;

    private ?int $offset = null;

    private Options $options;

    private SchemaConverter $schemaConverter;

    private readonly Filesystem $filesystem;

    /**
     * @param Path $path
     */
    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_parquet($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
        $this->schemaConverter = new SchemaConverter();
        $this->options = Options::default();
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $hydrator = $context->hydrator();
        $batchSize = $this->batchSize();
        $yielded = 0;

        $fileOffset = $this->offset ?? 0;
        $promisedSchema = $this->schema === null ? null : $this->schema();
        // undeclared, every file is read under the first file's schema, or the union of all of them
        $expected = $this->schema === null ? $this->derivedSchema($this->files(), $this->unionByName) : null;
        $target = $promisedSchema ?? $this->schema();

        $fileColumns = $this->fileColumns($this->filesystem, $this->path);

        foreach ($this->files() as $file) {
            // finally, not a close() per exit: the limit/STOP returns below and an abandoned
            // generator have to release the handle too (b73)
            try {
                $fileRows = $file->file->metadata()->rowsNumber();

                if ($fileOffset > $fileRows) {
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
                $matchTo = $promisedSchema === null && !$fileSchema->isSame($target) ? $target : null;

                $encoder = new ParquetEncoder($file->file->schema());

                $rawBatch = [];

                foreach ($file->file->values($this->columns, $this->pushedLimit(), $fileOffset) as $row) {
                    $rawBatch[] = $constants->fill($row);

                    if (count($rawBatch) >= $batchSize) {
                        $hydrated = $hydrator->hydrate($encoder->decode($rawBatch), $promisedSchema ?? $fileSchema);

                        if ($matchTo !== null) {
                            $hydrated = $hydrated->matchTo($matchTo);
                        }

                        $yielded += $hydrated->count();

                        $signal = yield $hydrated;

                        if ($signal === Signal::STOP) {
                            return;
                        }

                        $limit = $this->pushedLimit();

                        if ($limit !== null && $yielded >= $limit) {
                            return;
                        }

                        $rawBatch = [];
                    }
                }

                if ($rawBatch !== []) {
                    $hydrated = $hydrator->hydrate($encoder->decode($rawBatch), $promisedSchema ?? $fileSchema);

                    if ($matchTo !== null) {
                        $hydrated = $hydrated->matchTo($matchTo);
                    }

                    $yielded += $hydrated->count();

                    $signal = yield $hydrated;

                    if ($signal === Signal::STOP) {
                        return;
                    }

                    $limit = $this->pushedLimit();

                    if ($limit !== null && $yielded >= $limit) {
                        return;
                    }
                }

                $fileOffset = max($fileOffset - $fileRows, 0);
            } finally {
                $file->close();
            }
        }
    }

    public function schema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->declare(
            $this->schema ?? $this->derivedSchema($this->files(), $this->unionByName),
        );
    }

    /**
     * Reconcile every listed file's schema instead of trusting the first one.
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

    public function withByteOrder(ByteOrder $byteOrder): self
    {
        $this->byteOrder = $byteOrder;
        $this->derivedSchema = null;

        return $this;
    }

    /**
     * @param array<string> $columns
     */
    public function withColumns(array $columns): self
    {
        $this->columns = $columns;
        $this->derivedSchema = null;

        return $this;
    }

    public function withEngine(?ParquetEngine $engine): self
    {
        $this->engine = $engine;
        $this->derivedSchema = null;

        return $this;
    }

    public function withOffset(int $offset): self
    {
        if ($offset < 0) {
            throw new InvalidArgumentException('Offset must be greater or equal to 0');
        }

        $this->offset = $offset;

        return $this;
    }

    public function withOptions(Options $options): self
    {
        $this->options = $options;
        $this->derivedSchema = null;

        return $this;
    }

    /**
     * @return Generator<int, ParquetSourceFile>
     */
    private function files(): Generator
    {
        foreach ($this->sourceFiles($this->filesystem, $this->path) as $source) {
            $stream = $this->filesystem->readFrom($source->path);

            yield new ParquetSourceFile(
                (new Reader(byteOrder: $this->byteOrder, options: $this->options, engine: $this->engine))->readStream(
                    $stream,
                ),
                $stream,
                $source,
                $this->schemaConverter,
                array_values($this->columns),
            );
        }
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
