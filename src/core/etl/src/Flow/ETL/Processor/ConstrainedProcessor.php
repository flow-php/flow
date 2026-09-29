<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\BoundStep;
use Flow\ETL\Constraint;
use Flow\ETL\Exception\ConstraintViolationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

/**
 * Validates constraints on each row.
 */
final class ConstrainedProcessor implements Processor
{
    private int $rowIndex = 0;

    /**
     * @param array<Constraint> $constraints
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        public readonly array $constraints = [],
    ) {
        foreach ($constraints as $constraint) {
            // @mago-ignore analysis:impossible-condition
            if (!$constraint instanceof Constraint) {
                throw new InvalidArgumentException('Pipeline constraints must be of type Flow\ETL\Constraint');
            }
        }
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    /**
     * @param Generator<int, Rows> $rows
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        while ($rows->valid()) {
            /** @var Rows $batch */
            $batch = $rows->current();

            $violationIndex = null;
            $violated = null;

            // a constraint sees only the rows before the earliest violation found so far - the row-by-row order
            // in which an earlier constraint's violation stops the later ones
            foreach ($this->constraints as $constraint) {
                $index = $constraint->firstViolation(
                    $violationIndex === null ? $batch : $batch->slice(0, $violationIndex),
                );

                if ($index !== null) {
                    $violationIndex = $index;
                    $violated = $constraint;
                }
            }

            if ($violated !== null && $violationIndex !== null) {
                throw new ConstraintViolationException(
                    $violated->toString(),
                    $violated->violation($batch, $violationIndex),
                    $this->rowIndex + $violationIndex,
                );
            }

            $this->rowIndex += $batch->count();

            $signal = yield $batch;

            if ($signal === Signal::STOP) {
                $rows->send(Signal::STOP);

                return;
            }

            $rows->next();
        }
    }
}
