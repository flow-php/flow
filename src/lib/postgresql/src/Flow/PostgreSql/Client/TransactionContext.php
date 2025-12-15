<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

use Flow\PostgreSql\Client\Exception\TransactionException;

final class TransactionContext
{
    private int $nestingLevel = 0;

    public function begin() : ?string
    {
        $this->nestingLevel++;

        if ($this->nestingLevel === 1) {
            return null;
        }

        return $this->savepointName($this->nestingLevel);
    }

    public function commit() : ?string
    {
        if ($this->nestingLevel === 0) {
            throw TransactionException::noActiveTransaction();
        }

        if ($this->nestingLevel === 1) {
            $this->nestingLevel--;

            return null;
        }

        $name = $this->savepointName($this->nestingLevel);
        $this->nestingLevel--;

        return $name;
    }

    public function getNestingLevel() : int
    {
        return $this->nestingLevel;
    }

    public function isActive() : bool
    {
        return $this->nestingLevel > 0;
    }

    public function reset() : void
    {
        $this->nestingLevel = 0;
    }

    public function rollBack() : ?string
    {
        if ($this->nestingLevel === 0) {
            throw TransactionException::noActiveTransaction();
        }

        if ($this->nestingLevel === 1) {
            $this->nestingLevel = 0;

            return null;
        }

        $name = $this->savepointName($this->nestingLevel);
        $this->nestingLevel--;

        return $name;
    }

    private function savepointName(int $level) : string
    {
        return 'FLOW_' . $level;
    }
}
