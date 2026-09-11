<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Schema\Metadata;

final readonly class RawRowValues
{
    /**
     * @param array<string, mixed> $values
     * @param array<array-key, Metadata> $metadata
     */
    public function __construct(
        public array $values,
        public array $metadata = [],
    ) {}
}
