<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer\Rename;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function is_array;
use function str_replace;

final readonly class RenameReplaceColumnStrategy implements Describable, RenameColumnStrategy
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

        foreach ($schema->names() as $name) {
            $newName = str_replace($this->search, $this->replace, $name);

            if ($newName !== $name) {
                $renames[$name] = $newName;
            }
        }

        return $renames;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        // @mago-ignore analysis:mixed-assignment
        $search = $data['search'] ?? null;
        // @mago-ignore analysis:mixed-assignment
        $replace = $data['replace'] ?? null;

        return new self(
            is_array($search) ? type_list(type_string())->assert($search) : type_string()->assert($search),
            is_array($replace) ? type_list(type_string())->assert($replace) : type_string()->assert($replace),
        );
    }

    public function describe(): ?Description
    {
        return new Description(self::class, ['search' => $this->search, 'replace' => $this->replace]);
    }
}
