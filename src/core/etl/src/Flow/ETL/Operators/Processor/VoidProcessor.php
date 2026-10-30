<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;

/**
 * Discards all rows and yields an empty batch.
 */
final readonly class VoidProcessor implements Processor
{
    public function __construct(
        private ?Schema $declared = null,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            ($data['declared'] ?? null) === null ? null : Schema::fromArray(type_array()->assert($data['declared'])),
        );
    }

    public function describe(): ?Description
    {
        return new Description(self::class, ['declared' => $this->declared?->normalize()]);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($input), $input);
    }

    public function breaks(): bool
    {
        return true;
    }

    public function process(Generator $rows, Backend $backend): Generator
    {
        foreach ($rows as $_batch) {
        }

        // void() drops rows, not columns
        yield Rows::empty($this->declared ?? new Schema(), $backend);
    }
}
