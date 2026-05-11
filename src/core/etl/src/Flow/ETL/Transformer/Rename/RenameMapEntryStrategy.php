<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\Row;

final readonly class RenameMapEntryStrategy implements RenameEntryStrategy
{
    /**
     * @param array<string, string> $renames Map of old_name => new_name
     */
    public function __construct(
        private array $renames,
    ) {}

    public function rename(Row $row): Row
    {
        $rowRenames = \array_intersect_key($this->renames, \array_flip(\array_keys($row->entries()->toArray())));

        return $rowRenames === [] ? $row : $row->renameMany($rowRenames);
    }
}
