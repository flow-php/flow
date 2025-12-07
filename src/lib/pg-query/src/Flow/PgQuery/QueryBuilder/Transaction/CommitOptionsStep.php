<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Transaction;

interface CommitOptionsStep extends CommitFinalStep
{
    public function andChain() : CommitFinalStep;

    public function andNoChain() : CommitFinalStep;
}
