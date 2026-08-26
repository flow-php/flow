<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

interface MetadataColumnsExtractor
{
    public function withMetadataColumns(bool $addMetadataColumns): static;
}
