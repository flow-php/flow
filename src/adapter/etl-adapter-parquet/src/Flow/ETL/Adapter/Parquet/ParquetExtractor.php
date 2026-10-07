<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\File\DerivedSchema;
use Flow\ETL\Extractor\File\FileExtractor;
use Flow\ETL\Extractor\File\FileReading;
use Flow\ETL\Extractor\File\FileReadLoop;
use Flow\ETL\Extractor\File\MetadataColumnsExtractor;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SelfDescribingFile;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Generator;

use function array_values;
use function iterator_count;
use function iterator_to_array;
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

    /**
     * The first file the schema pass opened, kept for the read that follows.
     */
    private ?ParquetSourceFile $firstFile = null;

    private ?int $offset = null;

    private ?ParquetEngine $openedWith = null;

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

    public function __destruct()
    {
        $this->closeFirstFile();
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
        $sources = iterator_to_array($this->sourceFiles($this->filesystem, $this->path, $pathFilter), false);
        // every file is read under the first file's schema, or the union of all of them
        $target = $this->schema();
        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $batches = new ParquetFileBatches(
            $this->opener(),
            $fileColumns,
            $this->firstFile,
            $this->unionByName
                ? null
                : new DerivedSchema($this->derivedSchema($this->schemaFiles(), $this->unionByName), $this->derivedFrom),
        );
        $this->firstFile = null;

        try {
            yield from (new FileReadLoop($fileColumns, $target))->read(
                $sources,
                $batches,
                $this->batchSize(),
                $context->backend(),
                new ReadWindow($this->offset ?? 0, $limit),
            );
        } finally {
            $batches->close();
        }
    }

    public function schema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->declare($this->derivedSchema(
            $this->schemaFiles(),
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
        $this->closeFirstFile();
        $this->statistics = null;

        return $this;
    }

    public function partitionSchema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->partitions($this->schema());
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function withByteOrder(ByteOrder $byteOrder): self
    {
        $this->byteOrder = $byteOrder;
        $this->derivedSchema = null;
        $this->openedWith = null;
        $this->closeFirstFile();
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
        $this->closeFirstFile();
        $this->statistics = null;

        return $this;
    }

    public function withEngine(?ParquetEngine $engine): self
    {
        $this->engine = $engine;
        $this->derivedSchema = null;
        $this->openedWith = null;
        $this->closeFirstFile();
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
        $this->openedWith = null;
        $this->closeFirstFile();
        $this->statistics = null;

        return $this;
    }

    /**
     * The footers schema() reads - the first file's, or every file's under unionByName - read once whichever of the
     * two asks first, and scaled to the listing when only the first was read.
     */
    private function declare(): Statistics
    {
        return $this->derivedFooters($this->schemaFiles(), $this->unionByName)->of(
            iterator_count($this->sourceFiles($this->filesystem, $this->path)),
            $this->offset ?? 0,
        );
    }

    private function closeFirstFile(): void
    {
        $this->firstFile?->close();
        $this->firstFile = null;
    }

    /**
     * One open per file: with $keepFirst the first file stays open for the read that follows, which takes it for that
     * path instead of opening it again - or closes it when the read starts at another path.
     *
     * @return Generator<int, ParquetSourceFile>
     */
    private function files(Filter $pathFilter = new OnlyFiles(), bool $keepFirst = false): Generator
    {
        $kept = $this->firstFile;
        $this->firstFile = null;
        $first = true;

        foreach ($this->sourceFiles($this->filesystem, $this->path, $pathFilter) as $source) {
            if ($first && $kept !== null && $kept->source()->uri() !== $source->uri()) {
                $kept->close();
                $kept = null;
            }

            $file = ($first ? $kept : null) ?? $this->open($source);

            if ($keepFirst && $first) {
                $this->firstFile = $file;
            }

            $first = false;

            yield $file;
        }

        if ($first) {
            $kept?->close();
        }
    }

    /**
     * The files the schema fold reads: the first one through a ParquetSchemaPassFile, which the fold's close() leaves
     * open for the read that follows.
     *
     * @return Generator<int, SelfDescribingFile>
     */
    private function schemaFiles(): Generator
    {
        $first = true;

        foreach ($this->files(keepFirst: true) as $file) {
            yield $first ? new ParquetSchemaPassFile($file) : $file;
            $first = false;
        }
    }

    private function open(SourceFile $source): ParquetSourceFile
    {
        return $this->opener()->open($source);
    }

    private function opener(): ParquetSourceFileOpener
    {
        return new ParquetSourceFileOpener(
            $this->filesystem,
            $this->openedWith ??= $this->engine ?? new AdaptiveParquetEngine($this->byteOrder, $this->options),
            $this->options,
            $this->schemaConverter,
            array_values($this->columns),
        );
    }

    public function withSchema(Schema $schema): static
    {
        throw new InvalidArgumentException(
            'Parquet is a self-describing format and does not accept a schema; declaring one is not supported yet.',
        );
    }
}
