<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Functions\Evaluation\Filter;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

interface FileBatches extends Describable
{
    /**
     * @return Generator<int, CorruptedRecordException|Filter|Rows|SkippedRows, mixed, void>
     */
    public function batches(
        FileRead $read,
        Criteria $criteria,
        SourceFile $source,
        Schema $body,
        Backend $backend,
        ReadWindow $window,
    ): Generator;

    public function close(): void;
}
