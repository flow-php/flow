<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\{Constraint, FlowContext, Processor, Rows};
use Flow\ETL\Exception\{ConstraintViolationException, InvalidArgumentException};

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
    public function __construct(private readonly array $constraints = [])
    {
        foreach ($constraints as $constraint) {
            if (!$constraint instanceof Constraint) {
                throw new InvalidArgumentException('Pipeline constraints must be of type Flow\ETL\Constraint');
            }
        }
    }

    public function process(\Generator $rows, FlowContext $context) : \Generator
    {
        /** @var Rows $batch */
        foreach ($rows as $batch) {
            foreach ($batch->all() as $row) {
                foreach ($this->constraints as $constraint) {
                    if (!$constraint->isSatisfiedBy($row)) {
                        throw new ConstraintViolationException(
                            $constraint->toString(),
                            $constraint->violation($row),
                            $this->rowIndex
                        );
                    }
                }

                $this->rowIndex++;
            }

            yield $batch;
        }
    }
}
