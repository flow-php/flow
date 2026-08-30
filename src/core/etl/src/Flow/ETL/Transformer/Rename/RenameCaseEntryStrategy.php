<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\Schema;
use Flow\ETL\String\StringStyles;

final readonly class RenameCaseEntryStrategy implements RenameEntryStrategy
{
    public function __construct(
        private StringStyles $style,
    ) {}

    public function renames(Schema $schema): array
    {
        $renames = [];

        foreach ($schema->references()->names() as $name) {
            $newName = $this->style->convert($name);

            if ($newName !== $name) {
                $renames[$name] = $newName;
            }
        }

        return $renames;
    }
}
