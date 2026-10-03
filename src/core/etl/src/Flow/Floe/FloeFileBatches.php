<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Column\Backend;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Extractor\File\DerivedSchema;
use Flow\ETL\Extractor\File\FileColumns;
use Flow\ETL\Extractor\File\OffsetSkippingFileBatches;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Generator;

final readonly class FloeFileBatches implements OffsetSkippingFileBatches
{
    /**
     * @param null|DerivedSchema $derived the schema every file must match; null under unionByName
     */
    public function __construct(
        private Filesystem $filesystem,
        private Codec $codec,
        private int $chunkSize,
        private FileColumns $fileColumns,
        private ?DerivedSchema $derived,
    ) {}

    /**
     * Skips the whole file when the window's offset covers it; otherwise reads from the offset.
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
        $file = new FloeSourceFile(
            (new FloeReader($this->filesystem, $backend, $this->codec, $this->chunkSize))->read($source->path),
            $source,
        );

        try {
            $fileRows = $file->reader->totalRows();

            if ($window->offset >= $fileRows) {
                return $fileRows;
            }

            $this->derived?->refuseDivergence($file->source()->uri(), $file->schema());

            $fileBody = $this->fileColumns->withoutTail($file->schema());

            foreach ($file->reader->rows($batchSize, $window->offset, $window->limit) as $rows) {
                if (!$fileBody->isSame($file->schema())) {
                    $rows = $rows->project($fileBody, $backend);
                }

                yield $fileBody->isSame($body) ? $rows : $rows->matchTo($body, $backend);
            }

            return $window->offset;
        } finally {
            $file->close();
        }
    }
}
