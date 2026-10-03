<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\ETL\Column\Backend;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

interface FileBatches
{
    /**
     * Batches of one file under $body (file constants excluded), every column built by $backend.
     *
     * The return value is the rows of the window's offset this file consumed. A format that cannot skip rows is given
     * offset 0 and a limit that counts the offset still to skip, and returns 0: FileReadLoop slices the offset from its
     * rows. An OffsetSkippingFileBatches is given the offset and returns the rows it skipped. The limit is a hint a
     * format may exceed - FileReadLoop trims what goes past it.
     *
     * A file's own resources are released by the generator (finally). Resources that span files - a kept first file,
     * an inference sample - belong to the per-read object; the extractor that built it calls its close() in a finally
     * (ParquetFileBatches, ExcelFileBatches).
     *
     * @param int<1, max> $batchSize
     *
     * @return Generator<int, Rows, mixed, int>
     */
    public function batches(
        SourceFile $source,
        Schema $body,
        int $batchSize,
        Backend $backend,
        ReadWindow $window,
    ): Generator;
}
