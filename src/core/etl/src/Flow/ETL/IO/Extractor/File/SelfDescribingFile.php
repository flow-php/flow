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

    public function statistics(): Statistics;

    /**
     * @return list<ByteUnit>
     */
    public function units(): array;
}
