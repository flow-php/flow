<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\IO\Extractor;

use Flow\ETL\Adapter\Excel\IO\Inference\ExcelSniffer;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\DerivedFromFiles;
use Flow\ETL\IO\Extractor\File\FileColumnsSource;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\FileSplitReader;
use Flow\ETL\IO\Extractor\File\InferredColumns;
use Flow\ETL\IO\Extractor\File\MetadataColumnsExtractor;
use Flow\ETL\IO\Extractor\File\PartitionTypes;
use Flow\ETL\IO\Extractor\File\SourceFiles;
use Flow\ETL\IO\Extractor\InfersSchema;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\SplittableExtractor;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\IO\Inference\SampledUnits;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Inference\SchemaInferenceBuilder;
use Flow\ETL\IO\Inference\SchemaInferrer;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;
use Throwable;

use function count;
use function sprintf;

final class ExcelExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    InfersSchema,
    MetadataColumnsExtractor,
    SplittableExtractor
{
    private BatchSize $batchSize;

    private DerivedFromFiles $derived;

    private FileColumnsSource $fileColumns;

    private readonly SourceFiles $files;

    private SchemaInference $inference;

    private ExcelReadOptions $readOptions;

    /**
     * The sheets the last inference sampled, still open: the next extract() reads on from where the sample stopped
     * instead of parsing the sample again.
     */
    private ?ExcelSniffer $sampled = null;

    private int $sampledFiles = 0;

    /**
     * What the last inference's samples read; null until one ran. Kept apart from $sampled, which extract() takes.
     */
    private ?SampledUnits $sampledUnits = null;

    private ?Schema $schema = null;

    private ?Statistics $statistics = null;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        $filesystem->supports($path)
            || throw InvalidArgumentException::unsupportedPath($filesystem, $path, 'from_excel');

        $this->filesystem = $filesystem;

        if (!$this->path->isLocal()) {
            // We can't use resources (returned by \fopen) since they are not supported by the OpenSpout library.
            // They are not supported because OpenSpout library uses php built in ZipArchive library, which doesn't support resources, only local paths.
            throw new InvalidArgumentException(
                'Only local filesystem paths are supported by ExcelExtractor due to the limitation of underlying library.',
            );
        }

        $this->inference = new SchemaInference();
        $this->readOptions = new ExcelReadOptions();
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

    /**
     * An inference no extract() followed still holds its sample's open readers, and OpenSpout's file-based
     * shared-strings cache leaves its temp folder behind until a reader closes.
     */
    public function __destruct()
    {
        $this->sampled?->close();
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
        // a pending inference runs first and leaves its sample open; only the first extract() after an inference
        // reads on from that sample, every later one parses afresh
        $schema = $this->describe();
        $sampled = $this->sampled;
        $this->sampled = null;
        $fileColumns = $this->fileColumns->columns();

        yield from (new FileSplitReader(
            new FileRead($this->files, $fileColumns, $schema, $this->batchSize()),
            new ExcelFileBatches(
                new WorkbookReader($this->readOptions, new ExcelFormatDetector($this->files->filesystem)),
                $sampled,
                $this->schema === null && !$this->inference->unionByName
                    ? new InferredColumns(
                        $fileColumns->withoutTail($schema),
                        $fileColumns->tail(),
                        $this->inference,
                        $this->derived->from(),
                    )
                    : null,
            ),
        ))->extract($backend, $criteria);
    }

    public function reader(): FileSplitReader
    {
        $fileColumns = $this->fileColumns->columns();
        $schema = $this->describe();

        return new FileSplitReader(
            new FileRead($this->files, $fileColumns, $schema, $this->batchSize()),
            new ExcelFileBatches(
                new WorkbookReader($this->readOptions, new ExcelFormatDetector($this->files->filesystem)),
                null,
                $this->schema === null && !$this->inference->unionByName
                    ? new InferredColumns(
                        $fileColumns->withoutTail($schema),
                        $fileColumns->tail(),
                        $this->inference,
                        $this->derived->from(),
                    )
                    : null,
            ),
        );
    }

    public function inferSchema(SchemaInferenceBuilder $builder): static
    {
        $this->inference = $builder->build();
        $this->forgetInference();

        return $this;
    }

    public function schema(): Schema
    {
        $schema = $this->describe();
        $fileColumns = $this->fileColumns->columns();

        if ($this->schema === null && $fileColumns->withoutTail($schema)->count() === 0) {
            throw InferredSchemaException::nothingToInferFrom('from_excel()', sprintf(
                'no file matching "%s" carries a column',
                $this->path->uri(),
            ));
        }

        return $schema;
    }

    private function describe(): Schema
    {
        $fileColumns = $this->fileColumns->columns();

        if ($this->schema !== null) {
            return $fileColumns->declare($this->schema);
        }

        $derived = $this->derived->schema();

        if ($derived === null) {
            $sources = $this->files->of(new Criteria());
            $sniffer = new ExcelSniffer(
                new WorkbookReader($this->readOptions, new ExcelFormatDetector($this->filesystem)),
                $sources,
                $this->inference->candidates(),
            );

            try {
                $inferred = (new SchemaInferrer($this->inference))->infer($sniffer);
                $derived = $this->derived->learn($inferred->schema, $sniffer->header()->source ?? '');
                $this->sampledUnits = $inferred->units;
                $this->sampledFiles = count($sources);
            } catch (Throwable $failure) {
                $sniffer->close();

                throw $failure;
            }

            $this->sampled?->close();
            $this->sampled = $sniffer;
        }

        return $fileColumns->declare($fileColumns->withoutTail($derived));
    }

    public function partitionSchema(): Schema
    {
        return $this->fileColumns->columns()->partitions($this->schema ?? new Schema());
    }

    public function partitionTypes(PartitionTypes $types): static
    {
        $this->fileColumns = $this->fileColumns->withPartitionTypes($types);

        return $this;
    }

    public function prunes(ScalarFunction $conjunct): bool
    {
        return false;
    }

    /**
     * One split per admitted file: a workbook is a zip whose sheet streams have no seekable record boundary.
     *
     * @return non-empty-list<FileSplit>
     */
    public function splits(Criteria $criteria, int $targetSplitBytes): array
    {
        return $this->files->perFile($criteria->pathFilter);
    }

    public function refresh(): void
    {
        $this->files->refresh();
        $this->forgetInference();
    }

    public function source(): Path
    {
        return $this->path;
    }

    /**
     * The size is the listed, compressed byte count. Rows are known only when the sample schema inference already
     * read every row of every file: OpenSpout discards the row bound of a sheet's <dimension>.
     */
    public function statistics(): Statistics
    {
        if ($this->statistics !== null) {
            return $this->statistics;
        }
        $size = $this->files->bytes();

        if ($this->sampledUnits === null) {
            return new Statistics(rows: Cardinality::unknown(), size: $size);
        }

        // an extract() under a pushed path filter sampled only the files the filter kept
        return $this->statistics = new Statistics(
            rows: $this->sampledFiles === $this->files->count()
                ? $this->sampledUnits->exactRows($this->files->count())
                : Cardinality::unknown(),
            size: $size,
        );
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    public function withConvertEmptyToNull(bool $convertEmptyToNull): self
    {
        $this->readOptions = $this->readOptions->withConvertEmptyToNull($convertEmptyToNull);
        $this->forgetInference();

        return $this;
    }

    public function withHeader(bool $withHeader): self
    {
        $this->readOptions = $this->readOptions->withHeader($withHeader);
        $this->forgetInference();

        return $this;
    }

    public function withMetadataColumns(bool $addMetadataColumns): static
    {
        $this->fileColumns = $this->fileColumns->withMetadataColumns($addMetadataColumns);

        return $this;
    }

    public function withReader(ExcelReader $reader): self
    {
        $this->readOptions = $this->readOptions->withFormat($reader);
        $this->forgetInference();

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    public function withSheetName(string $sheetName): self
    {
        $this->readOptions = $this->readOptions->withSheetName($sheetName);
        $this->forgetInference();

        return $this;
    }

    /**
     * A changed read option or inference voids both the schema and the rows sampled under the old one.
     */
    private function forgetInference(): void
    {
        $this->derived = new DerivedFromFiles();
        $this->sampled?->close();
        $this->sampled = null;
        $this->sampledUnits = null;
        $this->sampledFiles = 0;
        $this->statistics = null;
    }
}
