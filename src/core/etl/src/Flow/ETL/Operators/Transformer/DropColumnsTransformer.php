<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

final readonly class DropColumnsTransformer implements Transformer
{
    private References $refs;

    public function __construct(string|Reference ...$names)
    {
        $this->refs = References::init(...$names);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(...$rebuild->of($data['refs'] ?? null, References::class)->all());
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['refs' => $this->refs]);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input->gracefulRemove(...$this->refs->names()));
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        return $rows->project($rows->schema()->gracefulRemove(...$this->refs->names()), $backend);
    }
}
