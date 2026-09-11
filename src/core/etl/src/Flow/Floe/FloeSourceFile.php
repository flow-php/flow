<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Extractor\SelfDescribingFile;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Schema;

final readonly class FloeSourceFile implements SelfDescribingFile
{
    public function __construct(
        public FloeStreamReader $reader,
        private SourceFile $source,
    ) {}

    public function close(): void
    {
        $this->reader->close();
    }

    public function schema(): Schema
    {
        return $this->reader->schema();
    }

    public function source(): SourceFile
    {
        return $this->source;
    }
}
