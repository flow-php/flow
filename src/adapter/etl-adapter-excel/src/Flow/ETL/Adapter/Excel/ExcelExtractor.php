<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\FileReading;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
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

use function array_diff;
use function array_values;
use function count;
use function iterator_to_array;
use function sprintf;

final class ExcelExtractor implements Extractor, FileExtractor, LimitableExtractor, MetadataColumnsExtractor
{
    use Limitable;
    use FileReading;

    private SchemaInference $inference;

    private ExcelReadOptions $readOptions;

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
        $this->resetLimit();
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $hydrator = $context->hydrator();
        $batchSize = $context->config->extractorBatchSize();
        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $sources = iterator_to_array($this->sourceFiles($this->filesystem, $this->path), false);
        $workbook = new WorkbookReader($this->readOptions, new ExcelFormatDetector($this->filesystem));
        $inferredFrom = '';

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
                } finally {
                    $sampler->close();
                }
            }

            $base = $fileColumns->withoutTail($derived);
        }

        $schema = $fileColumns->declare($base);
        $tail = $fileColumns->tail();
        $expected = $base->references()->names();

        foreach ($sources as $source) {
            $sheet = $workbook->sheet($source);

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

                    foreach ($hydrated as $hydratedRow) {
                        $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

                        $this->incrementReturnedRows();

                        if ($signal === Signal::STOP || $this->reachedLimit()) {
                            return;
                        }
                    }
                }

                if ($batch === []) {
                    continue;
                }

                $hydrated = $hydrator->hydrate($batch, $schema);

                foreach ($hydrated as $hydratedRow) {
                    $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

                    $this->incrementReturnedRows();

                    if ($signal === Signal::STOP || $this->reachedLimit()) {
                        return;
                    }
                }
            } finally {
                $sheet->close();
            }
        }
    }

    public function inferSchema(SchemaInferenceBuilder $builder): static
    {
        $this->inference = $builder->build();
        $this->derivedSchema = null;

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
            } finally {
                $sampler->close();
            }
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
        $this->derivedSchema = null;

        return $this;
    }

    public function withHeader(bool $withHeader): self
    {
        $this->readOptions = $this->readOptions->withHeader($withHeader);
        $this->derivedSchema = null;

        return $this;
    }

    public function withOffset(int $offset): self
    {
        $this->readOptions = $this->readOptions->withOffset($offset);
        $this->derivedSchema = null;

        return $this;
    }

    public function withReader(ExcelReader $reader): self
    {
        $this->readOptions = $this->readOptions->withFormat($reader);
        $this->derivedSchema = null;

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
        $this->derivedSchema = null;

        return $this;
    }
}
