<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

final class ConstraintViolationException extends RuntimeException
{
    public function __construct(
        private readonly string $constraint,
        private readonly string $violation,
        private readonly int $rowIndex,
    ) {
        parent::__construct("Constraint violation: {$this->constraint} - {$this->violation} in row: {$this->rowIndex}");
    }
}
