<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\FileReading;
use Flow\ETL\Extractor\InfersSchema;
use Flow\ETL\Extractor\LimitPushDown;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\PushesLimit;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferenceBuilder;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;
use Throwable;

use function array_diff;
use function array_values;
use function count;
use function iterator_to_array;
use function sprintf;

final class ExcelExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    InfersSchema,
    LimitPushDown,
    MetadataColumnsExtractor,
    RewindableExtractor
{
    use Batches;
    use PushesLimit;
    use FileReading;

    private SchemaInference $inference;

    private ExcelReadOptions $readOptions;

    /**
     * The sheets the last inference sampled, still open: the next extract() reads on from where the sample stopped
     * instead of parsing the sample again. DuckDB keeps its CSV sniffer's buffers for the scan the same way.
     */
    private ?WorkbookSampler $sampled = null;

    private ?Schema $schema = null;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_excel($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

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
    public function extract(FlowContext $context): Generator
    {
        $hydrator = $context->hydrator();
        $batchSize = $this->batchSize();
        $yielded = 0;
        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $sources = iterator_to_array($this->sourceFiles($this->filesystem, $this->path), false);
        $workbook = new WorkbookReader($this->readOptions, new ExcelFormatDetector($this->filesystem));
        $inferredFrom = '';
        // only the first extract() after an inference reads on from its sample; every later one parses afresh
        $sampled = $this->sampled;
        $this->sampled = null;

        try {
            if ($this->schema !== null) {
                $base = $this->schema;
            } else {
                // a local, not the property: withoutTail() takes a non-nullable Schema and no analyzer narrows a property
                $derived = $this->derivedSchema;

                if ($derived === null) {
                    $sampler = new WorkbookSampler($workbook, $sources);

                    try {
                        // one header() call: after infer() the sampler's sheets are closed and asking again reopens one
                        $header = $sampler->header();
                        $inferredFrom = $header->source ?? '';

                        $derived =
                            $this->derivedSchema = (new SchemaInferrer(
                                $this->inference,
                                new CellTypeNarrower($this->inference->candidates()),
                            ))->infer($header->names, $sampler->samples($this->inference->sampleSize));
                    } catch (Throwable $failure) {
                        $sampler->close();

                        throw $failure;
                    }

                    $sampled?->close();
                    $sampled = $sampler;
                }

                $base = $fileColumns->withoutTail($derived);
            }

            $schema = $fileColumns->declare($base);
            $tail = $fileColumns->tail();
            $expected = $base->references()->names();

            foreach ($sources as $source) {
                $sheet = $sampled?->take($source) ?? $workbook->sheet($source);

                try {
                    if ($this->schema === null && !$this->inference->unionByName) {
                        $columns = array_values(array_diff($sheet->columns(), $tail));

                        if (
                            $columns !== []
                            && (array_diff($columns, $expected) !== [] || array_diff($expected, $columns) !== [])
                        ) {
                            throw InferredSchemaException::columnsDiverge(
                                $source->uri(),
                                $inferredFrom,
                                $base,
                                $columns,
                                $this->inference,
                            );
                        }
                    }

                    // forFile() reads the PARTITION definitions, which only declare() creates - $base is the body
                    $constants = $fileColumns->forFile($source, $schema);
                    $batch = [];

                    foreach ($sheet->rows() as $rowValues) {
                        $batch[] = new RawRowValues($constants->fill($rowValues->values));

                        if (count($batch) < $batchSize) {
                            continue;
                        }

                        $hydrated = $hydrator->hydrate($batch, $schema);
                        $batch = [];

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

                    if ($batch === []) {
                        continue;
                    }

                    $hydrated = $hydrator->hydrate($batch, $schema);

                    $yielded += $hydrated->count();

                    $signal = yield $hydrated;

                    if ($signal === Signal::STOP) {
                        return;
                    }

                    $limit = $this->pushedLimit();

                    if ($limit !== null && $yielded >= $limit) {
                        return;
                    }
                } finally {
                    $sheet->close();
                }
            }
        } finally {
            $sampled?->close();
        }
    }

    public function inferSchema(SchemaInferenceBuilder $builder): static
    {
        $this->inference = $builder->build();
        $this->forgetInference();

        return $this;
    }

    public function schema(): Schema
    {
        $fileColumns = $this->fileColumns($this->filesystem, $this->path);

        if ($this->schema !== null) {
            return $fileColumns->declare($this->schema);
        }

        // a local, not the property: withoutTail() takes a non-nullable Schema and no analyzer narrows a property
        $derived = $this->derivedSchema;

        if ($derived === null) {
            $sampler = new WorkbookSampler(
                new WorkbookReader($this->readOptions, new ExcelFormatDetector($this->filesystem)),
                iterator_to_array($this->sourceFiles($this->filesystem, $this->path), false),
            );

            try {
                $derived =
                    $this->derivedSchema = (new SchemaInferrer(
                        $this->inference,
                        new CellTypeNarrower($this->inference->candidates()),
                    ))->infer($sampler->header()->names, $sampler->samples($this->inference->sampleSize));
            } catch (Throwable $failure) {
                $sampler->close();

                throw $failure;
            }

            $this->sampled?->close();
            $this->sampled = $sampler;
        }

        return $fileColumns->declare($fileColumns->withoutTail($derived));
    }

    public function source(): Path
    {
        return $this->path;
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

    public function withOffset(int $offset): self
    {
        $this->readOptions = $this->readOptions->withOffset($offset);
        $this->forgetInference();

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
        $this->derivedSchema = null;
        $this->sampled?->close();
        $this->sampled = null;
    }
}
