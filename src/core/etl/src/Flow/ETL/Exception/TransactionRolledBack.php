<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Loader;
use Throwable;

/**
 * A transaction child failed under a throw-only context and the transaction rolled back. $cause has NOT been offered
 * to the plan's error handler yet; the outer pipeline offers it once, against $loader.
 */
final class TransactionRolledBack extends RuntimeException
{
    public function __construct(
        public readonly Loader $loader,
        public readonly Throwable $cause,
    ) {
        parent::__construct($cause->getMessage(), 0, $cause);
    }
}
