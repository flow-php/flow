<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Rows;
use Throwable;

use function sprintf;

final class LimitReachedException extends RuntimeException
{
    public function __construct(
        public readonly int $limit,
        public readonly ?Rows $rows = null,
        ?Throwable $previous = null,
    ) {
        parent::__construct(sprintf('Limit of %d rows reached.', $limit), 0, $previous);
    }
}
