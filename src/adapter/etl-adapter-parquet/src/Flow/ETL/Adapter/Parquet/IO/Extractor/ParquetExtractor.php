<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Extractor;

use Flow\ETL\Adapter\Parquet\IO\Extractor\Pushdown\FilterTranslator;
use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\DerivedFromFiles;
use Flow\ETL\IO\Extractor\File\DerivedSchema;
use Flow\ETL\IO\Extractor\File\FileColumnsSource;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\FileSplitReader;
use Flow\ETL\IO\Extractor\File\MetadataColumnsExtractor;
use Flow\ETL\IO\Extractor\File\PartitionTypes;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Extractor\File\SourceFiles;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\SplittableExtractor;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Generator;

use function array_values;
use function sprintf;

final class ParquetExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    MetadataColumnsExtractor,
    SplittableExtractor
{
    private BatchSize $batchSize;

    private DerivedFromFiles $derived;

    private FileColumnsSource $fileColumns;

    private readonly SourceFiles $files;

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

    private ?ParquetEngine $openedWith = null;

    private Options $options;

    private ?Schema $schema = null;

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
        $filesystem->supports($path)
            || throw InvalidArgumentException::unsupportedPath($filesystem, $path, 'from_parquet');

        $this->filesystem = $filesystem;
        $this->schemaConverter = new SchemaConverter();
        $this->options = Options::default();
        $this->derived = new DerivedFromFiles();
        $this->files = new SourceFiles($filesystem, $this->path);
        $this->fileColumns = new FileColumnsSource($this->files);
        $this->batchSize = new BatchSize();
    }

    /**
     * @return int<1, max>
     */
    public function batchSize(): int
    {
        return $this->batchSize->size;
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
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        // a pending schema pass runs first and keeps the first file open; this scan reads on from it, and only this one
        $target = $this->schema();
        $reader = new FileSplitReader(
            new FileRead($this->files, $this->fileColumns->columns(), $target, $this->batchSize()),
            new ParquetFileBatches(
                $this->engineInUse(),
                $this->options,
                array_values($this->columns),
                $this->schema === null && !$this->unionByName
                    ? new DerivedSchema(
                        $this->derived->fold($this->schemaFiles(), $this->unionByName),
                        $this->derived->from(),
                    )
                    : null,
                $this->firstFile,
            ),
        );
        $this->firstFile = null;

        yield from $reader->extract($backend, $criteria);
    }

    public function reader(): FileSplitReader
    {
        // every file is read under the declared schema, the first file's schema, or the union of all of them
        $target = $this->schema();

        return new FileSplitReader(
            new FileRead($this->files, $this->fileColumns->columns(), $target, $this->batchSize()),
            new ParquetFileBatches(
                $this->engineInUse(),
                $this->options,
                array_values($this->columns),
                $this->schema === null && !$this->unionByName
                    ? new DerivedSchema(
                        $this->derived->fold($this->schemaFiles(), $this->unionByName),
                        $this->derived->from(),
                    )
                    : null,
            ),
        );
    }

    public function schema(): Schema
    {
        $fileColumns = $this->fileColumns->columns();

        if ($this->schema !== null) {
            return $fileColumns->declare(
                $this->columns === [] ? $this->schema : $this->schema->keep(...$this->columns),
            );
        }

        $derived = $this->derived->fold($this->schemaFiles(), $this->unionByName);

        if ($fileColumns->withoutTail($derived)->count() === 0) {
            throw InferredSchemaException::nothingToInferFrom('from_parquet()', sprintf(
                'no file matching "%s" carries a column',
                $this->path->uri(),
            ));
        }

        return $fileColumns->declare($derived);
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
        $this->derived = new DerivedFromFiles();
        $this->closeFirstFile();
        $this->statistics = null;

        return $this;
    }

    public function partitionSchema(): Schema
    {
        return $this->fileColumns->columns()->partitions($this->schema());
    }

    public function partitionTypes(PartitionTypes $types): static
    {
        $this->fileColumns = $this->fileColumns->withPartitionTypes($types);

        return $this;
    }

    public function prunes(ScalarFunction $conjunct): bool
    {
        return (
            (new FilterTranslator())->translate(
                [$conjunct],
                $this->fileColumns->columns()->withoutTail($this->schema()),
            ) !== []
        );
    }

    /**
     * Byte windows over every admitted file; a row group belongs to the window holding its midpoint.
     *
     * @return non-empty-list<FileSplit>
     */
    public function splits(Criteria $criteria, int $targetSplitBytes): array
    {
        return $this->files->byteWindows($targetSplitBytes, $criteria->pathFilter);
    }

    public function refresh(): void
    {
        $this->files->refresh();
        $this->derived = new DerivedFromFiles();
        $this->closeFirstFile();
        $this->statistics = null;
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    public function withByteOrder(ByteOrder $byteOrder): self
    {
        $this->byteOrder = $byteOrder;
        $this->derived = new DerivedFromFiles();
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
        $this->derived = new DerivedFromFiles();
        $this->closeFirstFile();
        $this->statistics = null;

        return $this;
    }

    public function withEngine(?ParquetEngine $engine): self
    {
        $this->engine = $engine;
        $this->derived = new DerivedFromFiles();
        $this->openedWith = null;
        $this->closeFirstFile();
        $this->statistics = null;

        return $this;
    }

    public function withMetadataColumns(bool $addMetadataColumns): static
    {
        $this->fileColumns = $this->fileColumns->withMetadataColumns($addMetadataColumns);

        return $this;
    }

    public function withOptions(Options $options): self
    {
        $this->options = $options;
        $this->derived = new DerivedFromFiles();
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
        return $this->derived->footers($this->schemaFiles(), $this->unionByName)->of($this->files->count());
    }

    private function closeFirstFile(): void
    {
        $this->firstFile?->close();
        $this->firstFile = null;
    }

    /**
     * The files the schema fold reads, one open per file: the first stays open for the read that follows, which takes
     * it for that path instead of opening it again - or closes it when the read starts at another path.
     *
     * @return Generator<int, ParquetSourceFile>
     */
    private function schemaFiles(): Generator
    {
        $kept = $this->firstFile;
        $this->firstFile = null;
        $first = true;

        foreach ($this->files->of(new Criteria()) as $source) {
            $file = ($first ? $kept?->forSource($source) : null) ?? $this->open($source);

            if ($first) {
                $this->firstFile = $file;
                $kept = null;
            }

            $first = false;

            yield $file;
        }

        $kept?->close();
    }

    private function open(SourceFile $source): ParquetSourceFile
    {
        return $this->opener()->open($source);
    }

    private function opener(): ParquetSourceFileOpener
    {
        return new ParquetSourceFileOpener(
            $this->filesystem,
            $this->engineInUse(),
            $this->options,
            $this->schemaConverter,
            array_values($this->columns),
        );
    }

    /**
     * The engine every file of this extractor is opened with: the one passed, else an adaptive one built once.
     */
    private function engineInUse(): ParquetEngine
    {
        return $this->openedWith ??= $this->engine ?? new AdaptiveParquetEngine($this->byteOrder, $this->options);
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
