<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Transaction;
use Throwable;

final class RecordingTransaction implements Transaction
{
    /**
     * @var list<string>
     */
    public array $log = [];

    /**
     * @var list<Throwable>
     */
    public array $rolledBackFor = [];

    public function __construct(
        private readonly ?Throwable $beginFailure = null,
        private readonly ?Throwable $commitFailure = null,
        private readonly ?Throwable $rollbackFailure = null,
    ) {}

    public function begin(): void
    {
        $this->log[] = 'begin';

        if ($this->beginFailure !== null) {
            throw $this->beginFailure;
        }
    }

    public function commit(): void
    {
        $this->log[] = 'commit';

        if ($this->commitFailure !== null) {
            throw $this->commitFailure;
        }
    }

    public function rollback(Throwable $cause): void
    {
        $this->log[] = 'rollback';
        $this->rolledBackFor[] = $cause;

        if ($this->rollbackFailure !== null) {
            throw $this->rollbackFailure;
        }
    }
}
