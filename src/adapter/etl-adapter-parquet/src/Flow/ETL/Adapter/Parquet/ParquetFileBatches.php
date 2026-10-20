<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Adapter\Parquet\Pushdown\FilterTranslator;
use Flow\ETL\Adapter\Parquet\Pushdown\TranslatedConjunct;
use Flow\ETL\Column\Backend;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Extractor\File\DerivedSchema;
use Flow\ETL\Extractor\File\FileColumns;
use Flow\ETL\Extractor\File\OffsetSkippingFileBatches;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Reader\ReadPlanner;
use Generator;

final class ParquetFileBatches implements OffsetSkippingFileBatches
{
    /**
     * @var list<TranslatedConjunct>
     */
    private readonly array $conjuncts;

    /**
     * @param null|ParquetSourceFile $firstFile the file the schema pass kept open, read instead of opened again
     * @param null|DerivedSchema $derived the schema every file must match; null under unionByName
     * @param list<ScalarFunction> $conjuncts FileScan::$conjuncts
     */
    public function __construct(
        private readonly ParquetSourceFileOpener $opener,
        private readonly FileColumns $fileColumns,
        private ?ParquetSourceFile $firstFile,
        private readonly ?DerivedSchema $derived,
        Schema $body,
        array $conjuncts = [],
    ) {
        $this->conjuncts = (new FilterTranslator())->translate($conjuncts, $body);
    }

    /**
     * Skips the whole file when the window's offset covers it; otherwise reads from the offset. A file of a narrower
     * schema is matched to $body. What the conjuncts a file answers exactly prove cannot match is skipped; FileReadLoop
     * drops the rest that does not match them.
     *
     * @param int<1, max> $batchSize
     *
     * @throws InferredSchemaException
     *
     * @return Generator<int, Rows, mixed, int>
     */
    public function batches(
        SourceFile $source,
        Schema $body,
        int $batchSize,
        Backend $backend,
        ReadWindow $window,
    ): Generator {
        $kept = $this->firstFile;
        $this->firstFile = null;

        if ($kept !== null && $kept->source()->uri() !== $source->uri()) {
            $kept->close();
            $kept = null;
        }

        $file = $kept ?? $this->opener->open($source);
        $opened = null;

        try {
            $footer = $file->file->reader()->footer();
            $fileRows = $footer->rowsNumber();

            if ($window->offset >= $fileRows) {
                return $fileRows;
            }

            $this->derived?->refuseDivergence($file->source()->uri(), $file->schema());

            $fileBody = $this->fileColumns->withoutTail($file->schema());
            $kept = [];

            foreach ($this->conjuncts as $conjunct) {
                if ($conjunct->readsAs($fileBody, $body)) {
                    $kept[] = $conjunct->filter;
                }
            }

            $opened = $file->open();

            // rows matching the conjuncts the file binds; FileReadLoop drops those the rest do not match
            foreach ($opened->batches(
                $fileBody,
                $batchSize,
                (new ReadPlanner())->plan(
                    $footer,
                    $file->file->indexes(),
                    $kept === []
                        ? null
                        : BoundFilter::bind(new All(...$kept), $footer->schema(), $file->file->options()),
                    $window->offset,
                    $window->limit,
                ),
                $backend,
            ) as $rows) {
                yield $fileBody->isSame($body) ? $rows : $rows->matchTo($body, $backend);
            }

            return $window->offset;
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
