<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

final readonly class FileColumnsSource
{
    public function __construct(
        private SourceFiles $files,
        private PartitionTypes $partitionTypes = new PartitionTypes(),
        private bool $metadataColumns = false,
    ) {}

    public function columns(): FileColumns
    {
        return new FileColumns($this->files->partitionNames(), $this->partitionTypes, $this->metadataColumns);
    }

    public function withMetadataColumns(bool $metadataColumns): self
    {
        return new self($this->files, $this->partitionTypes, $metadataColumns);
    }

    public function withPartitionTypes(PartitionTypes $partitionTypes): self
    {
        return new self($this->files, $partitionTypes, $this->metadataColumns);
    }
}
