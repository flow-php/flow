<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\Schema;

interface RenameEntryStrategy
{
    /**
     * @return array<string, string> map of current_name => new_name, empty when nothing is renamed
     */
    public function renames(Schema $schema): array;
}
