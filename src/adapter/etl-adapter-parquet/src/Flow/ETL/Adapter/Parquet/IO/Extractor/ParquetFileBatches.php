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
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\DerivedSchema;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\OffsetSkippingFileBatches;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\Reader\ReadPlan;
use Generator;
use SensitiveParameter;

use function array_diff;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

final class ParquetFileBatches implements OffsetSkippingFileBatches
{
    /**
     * The conjuncts of $translatedFor, translated once per read rather than per file.
     *
     * @var list<TranslatedConjunct>
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
     * Skips the whole file when the window's offset covers it; otherwise reads from the offset. A file of a narrower
     * schema is matched to $body. What the conjuncts a file answers exactly prove cannot match is skipped; FileReadLoop
     * drops the rest that does not match them. A split owns the row groups whose midpoint its byte window holds, and a
     * window without a length owns every row group.
     *
     * @throws InferredSchemaException
     *
     * @return Generator<int, Rows, mixed, int>
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

            if ($owned === null && $window->offset >= $fileRows) {
                return $fileRows;
            }

            if ($owned?->count === 0) {
                return 0;
            }

            $this->derived?->refuseDivergence($file->source()->uri(), $file->schema());

            $fileBody = $read->fileColumns->withoutTail($file->schema());
            $fileBody = $fileBody->gracefulRemove(...array_diff($fileBody->names(), $body->names()));
            $fileBoundConjuncts = [];

            foreach ($this->conjuncts as $conjunct) {
                if ($conjunct->readsAs($fileBody, $body)) {
                    $fileBoundConjuncts[] = $conjunct->filter;
                }
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
                    $owned->offset ?? $window->offset,
                    $window->limit,
                    $owned?->count,
                ),
                $backend,
            ) as $rows) {
                yield $fileBody->isSame($body) ? $rows : $rows->matchTo($body, $backend);
            }

            return $owned === null ? $window->offset : 0;
        } finally {
            $opened?->close();
            $file->close();
        }
    }

    /**
     * Closes the kept first file when the read never reached it.
     */
    public function close(): void
    {
        $this->firstFile?->close();
        $this->firstFile = null;
    }
}
