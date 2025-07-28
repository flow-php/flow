<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

final class OffsetReachedException extends RuntimeException
{
    public function __construct(public readonly int $offset, ?\Throwable $previous = null)
    {
        parent::__construct(\sprintf('Offset of %d rows reached.', $offset), 0, $previous);
    }
}
