<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer\Rename;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\String\StringStyles;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_string;

final readonly class RenameCaseColumnStrategy implements Describable, RenameColumnStrategy
{
    public function __construct(
        private StringStyles $style,
    ) {}

    public function renames(Schema $schema): array
    {
        $renames = [];

        foreach ($schema->names() as $name) {
            $newName = $this->style->convert($name);

            if ($newName !== $name) {
                $renames[$name] = $newName;
            }
        }

        return $renames;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(type_enum(StringStyles::class)->assert(constant(
            StringStyles::class . '::' . type_string()->assert($data['style'] ?? null),
        )));
    }

    public function describe(): ?Description
    {
        return new Description(self::class, ['style' => $this->style->name]);
    }
}
