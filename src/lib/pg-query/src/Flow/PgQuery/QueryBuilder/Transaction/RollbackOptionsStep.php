<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Transaction;

interface RollbackOptionsStep extends RollbackFinalStep
{
    public function andChain() : RollbackFinalStep;

    public function andNoChain() : RollbackFinalStep;

    public function toSavepoint(string $name) : RollbackFinalStep;
}
