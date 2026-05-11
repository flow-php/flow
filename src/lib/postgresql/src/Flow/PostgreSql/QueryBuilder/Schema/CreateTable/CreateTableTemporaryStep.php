<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\CreateTable;

/**
 * Intermediate step after calling temporary() that allows specifying ON COMMIT behavior.
 *
 * If no ON COMMIT method is called, the default behavior is ON COMMIT DROP for backward compatibility.
 */
interface CreateTableTemporaryStep extends CreateTableFinalStep
{
    /**
     * Delete all rows in temporary table at the end of each transaction, but preserve the table structure.
     */
    public function onCommitDeleteRows(): CreateTableFinalStep;

    /**
     * Drop temporary table at the end of each transaction.
     *
     * This is the default behavior when temporary() is called without specifying ON COMMIT.
     */
    public function onCommitDrop(): CreateTableFinalStep;

    /**
     * Preserve rows in temporary table at the end of each transaction.
     *
     * This is the most common behavior for temporary tables used across multiple transactions.
     */
    public function onCommitPreserveRows(): CreateTableFinalStep;
}
