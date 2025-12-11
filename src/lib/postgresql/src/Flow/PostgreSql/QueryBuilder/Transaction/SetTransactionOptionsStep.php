<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Transaction;

interface SetTransactionOptionsStep extends SetTransactionFinalStep
{
    public function deferrable() : self;

    public function isolationLevel(IsolationLevel $level) : self;

    public function notDeferrable() : self;

    public function readOnly() : self;

    public function readWrite() : self;

    public function snapshot(string $snapshotId) : SetTransactionFinalStep;
}
