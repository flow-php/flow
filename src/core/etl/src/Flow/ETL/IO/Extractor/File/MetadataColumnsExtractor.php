<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

interface MetadataColumnsExtractor
{
    public function withMetadataColumns(bool $addMetadataColumns): static;
}
