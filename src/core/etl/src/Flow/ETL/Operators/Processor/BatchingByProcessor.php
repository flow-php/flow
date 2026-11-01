<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\ComparableValues;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function Flow\Types\DSL\type_integer;

/**
 * Assumes input is pre-sorted by the batching column.
 */
final readonly class BatchingByProcessor implements Processor
{
    /**
     * @param null|int<1, max> $minSize
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        public Reference $column,
        public ?int $minSize = null,
    ) {
        // @mago-ignore analysis:invalid-operand,impossible-condition,redundant-comparison,redundant-logical-operation
        if ($this->minSize !== null && $this->minSize <= 0) {
            throw new InvalidArgumentException('Minimum batch size must be greater than 0, given: ' . $this->minSize);
        }
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['column'] ?? null, Reference::class),
            ($data['min_size'] ?? null) === null ? null : max(1, type_integer()->assert($data['min_size'])),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['column' => $this->column, 'min_size' => $this->minSize]);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public function breaks(): bool
    {
        return false;
    }

    /**
     * @param Generator<int, Rows> $rows
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function process(Generator $rows, Backend $backend): Generator
    {
        /** @var list<Rows> $parts */
        $parts = [];
        $buffered = 0;
        $currentValue = null;
        $hasValue = false;
        $comparable = new ComparableValues();
        $schema = null;

        while ($rows->valid()) {
            $schema ??= $rows->current()->schema();
            $batch = $rows->current()->matchTo($schema, $backend);
            $start = 0;

            if ($batch->isEmpty()) {
                $rows->next();

                continue;
            }

            // @mago-ignore analysis:mixed-assignment
            foreach ($comparable->equality($batch->column($this->column->base())) as $i => $value) {
                if (!$hasValue) {
                    // @mago-ignore analysis:mixed-assignment
                    $currentValue = $value;
                    $hasValue = true;
                }

                if ($value !== $currentValue) {
                    $size = $buffered + $i - $start;

                    if (($this->minSize === null || $size >= $this->minSize) && $size > 0) {
                        if ($i > $start) {
                            $parts[] = $batch->slice($start, $i - $start);
                        }

                        $signal = yield Rows::concat($parts[0]->schema(), $backend, ...$parts);

                        if ($signal === Signal::STOP) {
                            $rows->send(Signal::STOP);

                            return;
                        }

                        $parts = [];
                        $buffered = 0;
                        $start = $i;
                    }

                    // @mago-ignore analysis:mixed-assignment
                    $currentValue = $value;
                }
            }

            if ($start < $batch->count()) {
                $parts[] = $batch->slice($start, $batch->count() - $start);
                $buffered += $batch->count() - $start;
            }

            $rows->next();
        }

        if ($parts !== []) {
            yield Rows::concat($parts[0]->schema(), $backend, ...$parts);
        }
    }
}
