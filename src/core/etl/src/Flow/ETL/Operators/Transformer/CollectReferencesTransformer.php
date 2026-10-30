<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

final readonly class CollectReferencesTransformer implements Transformer
{
    public function __construct(
        private References $references,
    ) {}

    public function transform(Rows $rows, Backend $backend): Rows
    {
        foreach ($rows->schema()->names() as $name) {
            $this->references->add($name);
        }

        return $rows;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['references'] ?? null, References::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['references' => $this->references]);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }
}
