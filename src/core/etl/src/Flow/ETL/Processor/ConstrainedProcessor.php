<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Constraint;
use Flow\ETL\Exception\ConstraintViolationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Generator;

/**
 * Validates constraints on each row.
 *
 * @internal
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
        private readonly array $constraints = [],
    ) {
        foreach ($constraints as $constraint) {
            // @mago-ignore analysis:impossible-condition
            if (!$constraint instanceof Constraint) {
                throw new InvalidArgumentException('Pipeline constraints must be of type Flow\ETL\Constraint');
            }
        }
    }

    public function process(Generator $rows, FlowContext $context): Generator
    {
        foreach ($rows as $batch) {
            foreach ($batch->all() as $row) {
                foreach ($this->constraints as $constraint) {
                    if (!$constraint->isSatisfiedBy($row)) {
                        throw new ConstraintViolationException(
                            $constraint->toString(),
                            $constraint->violation($row),
                            $this->rowIndex,
                        );
                    }
                }

                $this->rowIndex++;
            }

            yield $batch;
        }
    }
}
