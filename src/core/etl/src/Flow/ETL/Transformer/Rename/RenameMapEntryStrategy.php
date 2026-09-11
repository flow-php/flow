<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\Schema;

use function array_flip;
use function array_intersect_key;

final readonly class RenameMapEntryStrategy implements RenameEntryStrategy
{
    /**
     * @param array<string, string> $renames Map of old_name => new_name
     */
    public function __construct(
        private array $renames,
    ) {}

    public function renames(Schema $schema): array
    {
        return array_intersect_key($this->renames, array_flip($schema->references()->names()));
    }
}
