<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Transformer\Stateful;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function Flow\Types\DSL\type_integer;

/**
 * Skips the first N rows.
 */
final readonly class OffsetProcessor implements Processor, Stateful
{
    /**
     * @param int<0, max> $offset
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        public int $offset,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->offset < 0) {
            throw new InvalidArgumentException('Offset must be greater than or equal to 0, given: ' . $this->offset);
        }
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(max(0, type_integer()->assert($data['offset'] ?? null)));
    }

    public function describe(): ?Description
    {
        return new Description(self::class, ['offset' => $this->offset]);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public function fresh(): static
    {
        return new self($this->offset);
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
        if ($this->offset === 0) {
            yield from $rows;

            return;
        }

        $skippedRows = 0;

        while ($rows->valid()) {
            /** @var Rows $batch */
            $batch = $rows->current();
            $currentBatchSize = $batch->count();
            $remainingToSkip = $this->offset - $skippedRows;

            if ($remainingToSkip >= $currentBatchSize) {
                $skippedRows += $currentBatchSize;
                $rows->next();

                continue;
            }

            if ($remainingToSkip > 0) {
                $batch = $batch->drop($remainingToSkip);
                $skippedRows += $remainingToSkip;
            }

            if ($batch->count() > 0) {
                $signal = yield $batch;

                if ($signal === Signal::STOP) {
                    $rows->send(Signal::STOP);

                    return;
                }
            }

            $rows->next();
        }
    }
}
