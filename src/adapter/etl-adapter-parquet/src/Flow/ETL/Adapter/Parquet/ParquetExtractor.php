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
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Validator\StrictValidator;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\Reader;
use Generator;

use function array_values;
use function iterator_count;
use function max;
use function sprintf;

final class ParquetExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    MetadataColumnsExtractor,
    RewindableExtractor
{
    use Batches;
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

    private ?Statistics $statistics = null;

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
    public function extract(FlowContext $context, ?int $limit = null, Filter $pathFilter = new OnlyFiles()): Generator
    {
        $backend = $context->backend();
        $batchSize = $this->batchSize();
        $yielded = 0;

        $fileOffset = $this->offset ?? 0;
        // every file is read under the first file's schema, or the union of all of them
        $expected = $this->derivedSchema($this->files(), $this->unionByName);
        $target = $this->schema();

        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $opener = ParquetOpeners::select($this->engine);

        foreach ($this->files($pathFilter) as $file) {
            $source = null;

            // finally, not a close() per exit: the limit/STOP returns below and an abandoned
            // generator have to release the handle too (b73)
            try {
                $fileRows = $file->file->metadata()->rowsNumber();

                if ($fileOffset > $fileRows) {
                    $fileOffset -= $fileRows;

                    continue;
                }

                if (!$this->unionByName) {
                    $validation = (new StrictValidator())->validate($expected, $file->schema());

                    if (!$validation->isValid()) {
                        throw InferredSchemaException::filesDiverge(
                            $file->source()->uri(),
                            $this->derivedFrom,
                            $validation,
                        );
                    }
                }

                $body = $fileColumns->withoutTail($file->schema());
                // R6: over the FILE's schema, never over schema()'s output
                $rowsSchema = $fileColumns->declare($file->schema());
                $constants = $fileColumns->forFile($file->source(), $rowsSchema);
                $matchTo = !$rowsSchema->isSame($target) ? $target : null;

                $source = $file->open($opener);

                foreach ($source->batches(
                    $body,
                    $batchSize,
                    $fileOffset,
                    $limit === null ? null : $limit - $yielded,
                    $backend,
                ) as $rows) {
                    $rows = $constants->fillRows($rows, $rowsSchema, $backend);

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

                $fileOffset = max($fileOffset - $fileRows, 0);
            } finally {
                $source?->close();
                $file->close();
            }
        }
    }

    public function schema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->declare($this->derivedSchema(
            $this->files(),
            $this->unionByName,
        ));
    }

    public function statistics(): Statistics
    {
        return $this->statistics ??= $this->declare();
    }

    /**
     * Reconcile every listed file's schema instead of trusting the first one.
     */
    public function unionByName(bool $union = true): self
    {
        $this->unionByName = $union;
        $this->derivedSchema = null;
        $this->statistics = null;

        return $this;
    }

    public function partitionSchema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->partitions(new Schema());
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function withByteOrder(ByteOrder $byteOrder): self
    {
        $this->byteOrder = $byteOrder;
        $this->derivedSchema = null;
        $this->statistics = null;

        return $this;
    }

    /**
     * @param array<string> $columns
     */
    public function withColumns(array $columns): self
    {
        $this->columns = $columns;
        $this->derivedSchema = null;
        $this->statistics = null;

        return $this;
    }

    public function withEngine(?ParquetEngine $engine): self
    {
        $this->engine = $engine;
        $this->derivedSchema = null;
        $this->statistics = null;

        return $this;
    }

    public function withOffset(int $offset): self
    {
        if ($offset < 0) {
            throw new InvalidArgumentException('Offset must be greater or equal to 0');
        }

        $this->offset = $offset;
        $this->statistics = null;

        return $this;
    }

    public function withOptions(Options $options): self
    {
        $this->options = $options;
        $this->derivedSchema = null;
        $this->statistics = null;

        return $this;
    }

    /**
     * The footers schema() reads - the first file's, or every file's under unionByName - read once whichever of the
     * two asks first, and scaled to the listing when only the first was read.
     */
    private function declare(): Statistics
    {
        return $this->derivedFooters($this->files(), $this->unionByName)->of(
            iterator_count($this->sourceFiles($this->filesystem, $this->path)),
            $this->offset ?? 0,
        );
    }

    /**
     * @return Generator<int, ParquetSourceFile>
     */
    private function files(Filter $pathFilter = new OnlyFiles()): Generator
    {
        foreach ($this->sourceFiles($this->filesystem, $this->path, $pathFilter) as $source) {
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
        throw new InvalidArgumentException(
            'Parquet is a self-describing format and does not accept a schema; declaring one is not supported yet.',
        );
    }
}
