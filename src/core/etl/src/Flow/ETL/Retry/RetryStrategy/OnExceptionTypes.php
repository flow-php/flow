<?php

declare(strict_types=1);

namespace Flow\ETL\Retry\RetryStrategy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Retry\RetryStrategy;

final readonly class OnExceptionTypes implements RetryStrategy
{
    /**
     * @var array<class-string<\Throwable>>
     */
    private array $exceptionTypes;

    /**
     * @param array<class-string<\Throwable>> $exceptionTypes
     */
    public function __construct(array $exceptionTypes, private int $limit)
    {
        if ($exceptionTypes === []) {
            throw new InvalidArgumentException('Exception types cannot be empty. Use AnyThrowable strategy to retry on any throwable.');
        }

        if ($limit <= 0) {
            throw new InvalidArgumentException('Retry limit must be greater than 0');
        }

        foreach ($exceptionTypes as $exceptionType) {
            if (!\is_string($exceptionType) || (!\class_exists($exceptionType) && !\interface_exists($exceptionType))) {
                throw new InvalidArgumentException("Class '{$exceptionType}' does not exist");
            }

            if (!\is_subclass_of($exceptionType, \Throwable::class) && $exceptionType !== \Throwable::class) {
                throw new InvalidArgumentException("Class '{$exceptionType}' is not a Throwable");
            }
        }

        $this->exceptionTypes = $exceptionTypes;
    }

    public function shouldRetry(\Throwable $exception, int $attemptNumber) : bool
    {
        if ($attemptNumber > $this->limit) {
            return false;
        }

        foreach ($this->exceptionTypes as $exceptionType) {
            if ($exception instanceof $exceptionType) {
                return true;
            }
        }

        return false;
    }
}
