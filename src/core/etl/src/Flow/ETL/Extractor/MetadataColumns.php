<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

trait MetadataColumns
{
    private bool $addMetadataColumns = false;

    public function withMetadataColumns(bool $addMetadataColumns): static
    {
        $this->addMetadataColumns = $addMetadataColumns;

        return $this;
    }
}
