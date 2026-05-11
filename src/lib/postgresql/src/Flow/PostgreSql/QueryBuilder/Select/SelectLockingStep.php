<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

interface SelectLockingStep extends SelectFinalStep
{
    /**
     * @param string ...$tables
     */
    public function forKeyShare(string ...$tables): SelectFinalStep;

    /**
     * @param string ...$tables
     */
    public function forNoKeyUpdate(string ...$tables): SelectFinalStep;

    /**
     * @param string ...$tables
     */
    public function forShare(string ...$tables): SelectFinalStep;

    /**
     * @param string ...$tables
     */
    public function forUpdate(string ...$tables): SelectFinalStep;

    /**
     * @param string ...$tables
     */
    public function forUpdateSkipLocked(string ...$tables): SelectFinalStep;
}
