<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\Row;
use Flow\ETL\String\StringStyles;

final readonly class RenameCaseEntryStrategy implements RenameEntryStrategy
{
    public function __construct(
        private StringStyles $style,
    ) {}

    public function rename(Row $row): Row
    {
        $renames = [];

        foreach ($row->entries()->all() as $entry) {
            $newName = $this->style->convert($entry->name());

            if ($newName !== $entry->name()) {
                $renames[$entry->name()] = $newName;
            }
        }

        return $renames === [] ? $row : $row->renameMany($renames);
    }
}
