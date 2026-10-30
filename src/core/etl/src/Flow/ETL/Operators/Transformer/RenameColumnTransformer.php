<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

use function Flow\Types\DSL\type_string;

final readonly class RenameColumnTransformer implements Transformer
{
    public function __construct(
        private string $from,
        private string $to,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        if ($this->from === $this->to) {
            return new BoundStep($this, $input);
        }

        return new BoundStep($this, $input->rename($this->from, $this->to));
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(type_string()->assert($data['from'] ?? null), type_string()->assert($data['to'] ?? null));
    }

    public function describe(): ?Description
    {
        return new Description(self::class, ['from' => $this->from, 'to' => $this->to]);
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        if ($this->from === $this->to) {
            return $rows;
        }

        return $rows->withSchema($rows->schema()->rename($this->from, $this->to));
    }
}
