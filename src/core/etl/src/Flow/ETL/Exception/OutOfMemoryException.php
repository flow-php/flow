<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Rows;
use Throwable;

final class OutOfMemoryException extends RuntimeException
{
    /**
     * @param null|Rows $collectedRows
     */
    public function __construct(
        public readonly ?Rows $collectedRows = null,
        ?Throwable $previous = null,
    ) {
        parent::__construct('Memory limit exceeded', 0, $previous);
    }
}
