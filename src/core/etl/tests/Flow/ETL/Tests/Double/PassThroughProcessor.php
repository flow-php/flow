<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

final readonly class PassThroughProcessor implements Processor
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return new Description(self::class, []);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public function breaks(): bool
    {
        return false;
    }

    public function process(Generator $rows, Backend $backend): Generator
    {
        yield from $rows;
    }
}
