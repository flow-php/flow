<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer\Rename;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

use function array_flip;
use function array_intersect_key;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final readonly class RenameMapColumnStrategy implements Describable, RenameColumnStrategy
{
    /**
     * @param array<array-key, string> $renames Map of old_name => new_name
     */
    public function __construct(
        private array $renames,
    ) {}

    public function renames(Schema $schema): array
    {
        return array_intersect_key($this->renames, array_flip($schema->names()));
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(type_map(type_string(), type_string())->assert($data['renames'] ?? null));
    }

    public function describe(): ?Description
    {
        return new Description(self::class, ['renames' => $this->renames]);
    }
}
