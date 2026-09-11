<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

trait DeclaresPartitionTypes
{
    private PartitionTypes $partitionTypes;

    public function declaredPartitionTypes(): PartitionTypes
    {
        return $this->partitionTypes ??= new PartitionTypes();
    }

    public function partitionTypes(PartitionTypes $types): static
    {
        $this->partitionTypes = $types;

        return $this;
    }
}
