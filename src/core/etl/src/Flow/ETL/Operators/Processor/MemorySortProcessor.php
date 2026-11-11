<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Sort\SortColumn;
use Generator;
use SensitiveParameter;

use function array_values;
use function Flow\Types\DSL\type_array;
use function max;

final readonly class MemorySortProcessor implements Distributable
{
    public function __construct(
        /** @var list<SortColumn> */
        public array $columns,
        private ?Schema $declared = null,
        private Progress $progress = new VoidProgress(),
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            array_values($rebuild->all($data['columns'] ?? null, SortColumn::class)),
            ($data['declared'] ?? null) === null ? null : Schema::fromArray(type_array()->assert($data['declared'])),
            $rebuild->progress(StageName::sort, $data['progress'] ?? null),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'columns' => $this->columns,
            'declared' => $this->declared?->normalize(),
            'progress' => $this->progress instanceof Describable ? $this->progress : new VoidProgress(),
        ]);
    }

    public function breaks(): bool
    {
        return true;
    }

    public function process(Generator $rows, Backend $backend): Generator
    {
        /** @var list<Rows> $buffer */
        $buffer = [];
        $maxSize = 1;

        $schema = null;

        foreach ($rows as $batch) {
            $schema ??= $batch->schema();
            if ($batch->isEmpty()) {
                continue;
            }

            $maxSize = max($batch->count(), $maxSize);
            $buffer[] = $batch->matchTo($this->declared ?? $schema, $backend);
            $this->progress->advanced($batch->count());
        }

        $all = Rows::concat($this->declared ?? $schema ?? new Schema(), $backend, ...$buffer);

        if ($all->isEmpty()) {
            yield from $all->chunks($maxSize);
            $this->progress->done();

            return;
        }

        foreach ($backend->sort($all, $this->columns)->chunks($maxSize) as $chunk) {
            $this->progress->busy();

            yield $chunk;
        }

        $this->progress->done();
    }

    public function distribution(): ?Distribution
    {
        return $this->columns === [] ? null : Distribution::ordered($this->columns, $this->progress);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($this->columns, $input, $this->progress), $input);
    }
}
