<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\Schema;

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

    public function renames(Schema $schema): array
    {
        $renames = [];

        foreach ($schema->references()->names() as $name) {
            $newName = str_replace($this->search, $this->replace, $name);

            if ($newName !== $name) {
                $renames[$name] = $newName;
            }
        }

        return $renames;
    }
}
