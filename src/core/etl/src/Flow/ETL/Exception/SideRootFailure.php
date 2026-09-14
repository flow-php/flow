<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Throwable;

/**
 * $cause already went through the side pipeline's own error handler, which is not always the plan's - a catcher must
 * ensure the plan's handler sees $cause exactly once, not skip it or offer it twice.
 */
final class SideRootFailure extends RuntimeException
{
    public function __construct(
        public readonly Throwable $cause,
    ) {
        parent::__construct($cause->getMessage(), 0, $cause);
    }
}
