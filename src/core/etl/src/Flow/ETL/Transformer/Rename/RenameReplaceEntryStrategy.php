<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\Row;

use function str_replace;

final readonly class RenameReplaceEntryStrategy implements RenameEntryStrategy
{
    /**
     * @param array<string>|string $search
     * @param array<string>|string $replace
     */
    public function __construct(
        private string|array $search,
        private string|array $replace,
    ) {}

    public function rename(Row $row): Row
    {
        $renames = [];

        foreach ($row->entries()->all() as $entry) {
            $newName = str_replace($this->search, $this->replace, $entry->name());

            if ($newName !== $entry->name()) {
                $renames[$entry->name()] = $newName;
            }
        }

        return $renames === [] ? $row : $row->renameMany($renames);
    }
}
