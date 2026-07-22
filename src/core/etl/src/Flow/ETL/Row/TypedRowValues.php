<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

final readonly class TypedRowValues
{
    /**
     * @param array<string, mixed> $values
     * @param array<string, Type<mixed>> $types
     * @param array<string, Metadata> $metadata
     */
    public function __construct(
        public array $values,
        public array $types,
        public array $metadata = [],
    ) {}
}
