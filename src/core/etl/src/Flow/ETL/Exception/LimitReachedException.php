<?php declare(strict_types=1);

namespace Flow\ETL\Exception;

final class LimitReachedException extends RuntimeException
{
    public function __construct(public readonly int $limit, ?\Throwable $previous = null)
    {
        parent::__construct(\sprintf('Limit of %d rows reached.', $limit), 0, $previous);
    }
}
