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
 * Collects all rows from the upstream generator into a single batch.
 *
 * This processor consumes the entire input generator and yields
 * all rows as a single Rows batch. Use with caution on large datasets
 * as it loads everything into memory.
 */
final readonly class CollectingProcessor implements Processor
{
    public function __construct(
        public ?Schema $declared = null,
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
        $collected = null;

        foreach ($rows as $batch) {
            // an empty part adds nothing, whatever its schema
            $collected = match (true) {
                $collected === null, $collected->isEmpty() => $batch,
                $batch->isEmpty() => $collected,
                default => Rows::concat($collected->schema(), $backend, $collected, $batch),
            };
        }

        yield $collected ?? Rows::empty($this->declared ?? new Schema(), $backend);
    }
}
