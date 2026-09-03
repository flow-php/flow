<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor\SelfDescribingFile;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Schema;

final class FakeSelfDescribingFile implements SelfDescribingFile
{
    public bool $closed = false;

    public function __construct(
        private readonly Schema $schema,
        private readonly SourceFile $source,
        private readonly bool $describingThrows = false,
    ) {}

    public function close(): void
    {
        $this->closed = true;
    }

    public function schema(): Schema
    {
        if ($this->describingThrows) {
            throw new RuntimeException('this file cannot describe itself');
        }

        return $this->schema;
    }

    public function source(): SourceFile
    {
        return $this->source;
    }
}
