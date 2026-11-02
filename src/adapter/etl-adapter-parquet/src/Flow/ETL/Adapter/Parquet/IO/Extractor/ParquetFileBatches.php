<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Extractor;

use Flow\ETL\Adapter\Parquet\IO\ConfiguredParquetEngine;
use Flow\ETL\Adapter\Parquet\IO\Extractor\Pushdown\FilterTranslator;
use Flow\ETL\Adapter\Parquet\IO\Extractor\Pushdown\TranslatedConjunct;
use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Functions\Evaluation\Filter;
use Flow\ETL\Functions\ReferencedColumns;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\DerivedSchema;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\OffsetSkippingFileBatches;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SkippedRows;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Filesystem\ScalarFunctionFilter;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\FileStatus;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader\ReadPlan;
use Generator;
use SensitiveParameter;

use function array_diff;
use function array_flip;
use function array_intersect;
use function array_intersect_key;
use function count;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

final class ParquetFileBatches implements OffsetSkippingFileBatches
{
    /**
     * @var array<int, TranslatedConjunct> translated once per read, not per file
     */
    private array $conjuncts = [];

    private ?Criteria $translatedFor = null;

    private readonly SchemaConverter $schemaConverter;

    /**
     * @param list<string> $columns the columns to read, every one when empty
     * @param null|DerivedSchema $derived the schema every file must match; null under unionByName
     * @param null|ParquetSourceFile $firstFile the file the schema pass kept open, read instead of opened again; never
     *                                          described
     */
    public function __construct(
        private readonly ParquetEngine $engine,
        private readonly Options $options,
        private readonly array $columns,
        private readonly ?DerivedSchema $derived,
        private ?ParquetSourceFile $firstFile = null,
    ) {
        $this->schemaConverter = new SchemaConverter();
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $derived = type_optional(type_array())->assert($data['derived'] ?? null);

        return new self(
            $rebuild->of($data['engine'] ?? null, ConfiguredParquetEngine::class)->engine(),
            Options::fromArray(type_array()->assert($data['options'] ?? null)),
            type_list(type_string())->assert($data['columns'] ?? null),
            $derived === null ? null : DerivedSchema::fromArray($derived),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'engine' => new ConfiguredParquetEngine($this->engine),
            'options' => $this->options->normalize(),
            'columns' => $this->columns,
            'derived' => $this->derived?->normalize(),
        ]);
    }

    /**
     * @throws InferredSchemaException
     *
     * @return Generator<int, Filter|Rows|SkippedRows, mixed, void>
     */
    public function batches(
        FileRead $read,
        Criteria $criteria,
        SourceFile $source,
        Schema $body,
        Backend $backend,
        ReadWindow $window,
    ): Generator {
        if ($this->translatedFor !== $criteria) {
            $this->conjuncts = (new FilterTranslator())->translate($criteria->conjuncts, $body);
            $this->translatedFor = $criteria;
        }

        $split = $criteria->split;
        $file = $this->firstFile?->forSource($source) ?? (new ParquetSourceFileOpener(
            $read->files->filesystem,
            $this->engine,
            $this->options,
            $this->schemaConverter,
            $this->columns,
        ))->open($source);
        $this->firstFile = null;
        $opened = null;

        try {
            $footer = $file->file->reader()->footer();
            $fileRows = $footer->rowsNumber();
            $owned = $split?->length === null ? null : $split->owns($file->units());

            if ($owned?->count === 0) {
                return;
            }

            $this->derived?->refuseDivergence($file->source()->uri(), $file->schema());

            $fileBody = $read->fileColumns->withoutTail($file->schema());
            $fileBody = $fileBody->gracefulRemove(...array_diff($fileBody->names(), $body->names()));
            $fileBoundConjuncts = [];
            $residual = [];
            $mayHoldNaN = [];

            foreach ($footer->metadata()->columnChunks() as $chunk) {
                $column = $footer->schema()->get($chunk->flatPath());

                // a missing count proves nothing, so the column may hold NaN
                if (
                    $column instanceof FlatColumn
                    && PhysicalOrder::forColumn($column)->isFloatingPoint()
                    && $chunk->statistics()?->nanCount !== 0
                ) {
                    $mayHoldNaN[$chunk->flatPath()] = true;
                }
            }

            $partitions = $read->fileColumns->partitions($read->schema);
            $referenced = new ReferencedColumns();

            foreach ($criteria->conjuncts as $position => $conjunct) {
                $translated = $this->conjuncts[$position] ?? null;
                $partitionOnly =
                    $translated === null
                    && $partitions->names() !== []
                    && array_diff($referenced->in($conjunct)->names(), $partitions->names()) === [];

                // a conjunct over partition columns is one value per file, so TRUE for the file is TRUE for every row
                if (
                    $partitionOnly
                    && (new ScalarFunctionFilter($conjunct, $partitions, $backend))->accept(
                        new FileStatus($source->path, true),
                    )
                ) {
                    continue;
                }

                if (
                    $translated === null
                    || !$translated->readsAs($fileBody, $body)
                    || array_intersect_key($mayHoldNaN, array_flip($translated->nanSensitiveColumns)) !== []
                ) {
                    $residual[] = $conjunct;

                    continue;
                }

                $fileBoundConjuncts[] = $translated->filter;

                if (
                    !$translated->exact
                    || !BoundFilter::bind(
                        $translated->filter,
                        $footer->schema(),
                        $file->file->options(),
                    )->answersEveryConjunct()
                ) {
                    $residual[] = $conjunct;
                }
            }

            $evaluatesNothing = $residual === [] && $fileBoundConjuncts === [];

            // an offset beside a conjunct left to evaluate counts matching rows, which the footer does not know
            if ($owned === null && $evaluatesNothing && $window->offset >= $fileRows) {
                yield new SkippedRows($fileRows);

                return;
            }

            if (count($residual) < count($criteria->conjuncts)) {
                yield Filter::bind($read->schema, ...$residual);
            }

            // as output a column decodes on every row; left to the engine, only where statistics leave its filter open
            if ($criteria->columns !== null) {
                $kept = $criteria->columns;

                foreach ($residual as $conjunct) {
                    $kept = [...$kept, ...$referenced->in($conjunct)->names()];
                }

                $fileBody = $fileBody->keep(...array_intersect($fileBody->names(), $kept));
                $body = $body->keep(...array_intersect($body->names(), $kept));
            }

            $takesWindow = $residual === [];
            $rawOffset = $evaluatesNothing ? $window->offset : 0;

            if ($rawOffset > 0) {
                yield new SkippedRows($rawOffset);
            }

            $opened = $file->open();

            foreach ($opened->batches(
                $fileBody,
                $read->batchSize,
                ReadPlan::forFilter(
                    $footer,
                    $file->file->indexes(),
                    $fileBoundConjuncts === []
                        ? null
                        : BoundFilter::bind(new All(...$fileBoundConjuncts), $footer->schema(), $file->file->options()),
                    $owned->offset ?? $rawOffset,
                    $takesWindow ? $window->limit : null,
                    $owned?->count,
                    $takesWindow ? $window->offset - $rawOffset : 0,
                ),
                $backend,
            ) as $batch) {
                yield $batch instanceof SkippedRows || $fileBody->isSame($body)
                    ? $batch
                    : $batch->matchTo($body, $backend);
            }
        } finally {
            $opened?->close();
            $file->close();
        }
    }

    public function close(): void
    {
        $this->firstFile?->close();
        $this->firstFile = null;
    }
}
