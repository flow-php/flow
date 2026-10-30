<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Schema;

interface SelfDescribingFile
{
    public function close(): void;

    public function schema(): Schema;

    public function source(): SourceFile;

    /**
     * What this one file's own metadata declares, read from what schema() already opened.
     */
    public function statistics(): Statistics;

    /**
     * The format's own units of rows - row groups, sections - as byte ranges in file order, so a byte window can be
     * told which rows it owns. Read from what schema() already opened.
     *
     * @return list<ByteUnit>
     */
    public function units(): array;
}
