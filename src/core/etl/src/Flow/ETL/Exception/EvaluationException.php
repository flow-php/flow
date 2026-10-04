<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Exception as BaseException;

use function Flow\Types\DSL\type_instance_of;
use function sprintf;

final class EvaluationException extends InvalidArgumentException
{
    public function __construct(
        public readonly int $rowIndex,
        BaseException $cause,
    ) {
        parent::__construct(sprintf('%s (row %d)', $cause->getMessage(), $rowIndex), 0, $cause);
    }

    /**
     * $cause at $rowIndex of the caller's batch; an EvaluationException cause is re-indexed, never nested.
     */
    public static function at(int $rowIndex, BaseException $cause): self
    {
        return $cause instanceof self
            ? new self($rowIndex, type_instance_of(BaseException::class)->assert($cause->getPrevious()))
            : new self($rowIndex, $cause);
    }
}
