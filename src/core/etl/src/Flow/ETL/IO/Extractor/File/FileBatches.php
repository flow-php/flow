<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

interface FileBatches extends Describable
{
    /**
     * Batches of one file of $read under $body (file constants excluded), every column built by $backend, in
     * $read->batchSize rows; $criteria carries the split and the conjuncts.
     *
     * The return value is the rows of the window's offset this file consumed. A format that cannot skip rows is given
     * offset 0 and a limit that counts the offset still to skip, and returns 0: FileReadLoop slices the offset from its
     * rows. An OffsetSkippingFileBatches is given the offset and returns the rows it skipped. The limit is a hint a
     * format may exceed - FileReadLoop trims what goes past it - and null when the read carries conjuncts, whose rows
     * FileReadLoop counts. FileReadLoop applies the conjuncts to every row returned; a format given them when built may
     * skip what they prove cannot match.
     *
     * A file's own resources are released by the generator (finally); those that span files go at close(). A corrupted
     * record is handed up as its CorruptedRecordException, in record order, never thrown.
     *
     * @return Generator<int, CorruptedRecordException|Rows, mixed, int>
     */
    public function batches(
        FileRead $read,
        Criteria $criteria,
        SourceFile $source,
        Schema $body,
        Backend $backend,
        ReadWindow $window,
    ): Generator;

    /**
     * Releases what spans the files of one read (a kept first file, an inference sample); FileSplitReader calls it
     * when the read ends.
     */
    public function close(): void;
}
