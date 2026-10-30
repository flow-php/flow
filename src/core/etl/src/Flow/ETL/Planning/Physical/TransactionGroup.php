<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Physical;

use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Transaction;

final readonly class TransactionGroup
{
    /**
     * @param list<Loader|BranchPipeline> $children the writes one transaction wraps
     */
    public function __construct(
        public Transaction $transaction,
        public array $children,
    ) {}
}
