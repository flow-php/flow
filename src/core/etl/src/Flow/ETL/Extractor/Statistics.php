<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Cardinality;

final readonly class Statistics
{
    public function __construct(
        public Cardinality $rows = new Cardinality(),
        public Cardinality $size = new Cardinality(),
    ) {}

    public function merge(self $other): self
    {
        return new self($this->rows->merge($other->rows), $this->size->merge($other->size));
    }
}
