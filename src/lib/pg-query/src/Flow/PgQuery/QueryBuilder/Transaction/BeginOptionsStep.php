<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Transaction;

interface BeginOptionsStep extends BeginFinalStep
{
    public function deferrable() : self;

    public function isolationLevel(IsolationLevel $level) : self;

    public function notDeferrable() : self;

    public function readOnly() : self;

    public function readWrite() : self;
}
