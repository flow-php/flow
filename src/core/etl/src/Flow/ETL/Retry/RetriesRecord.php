<?php

declare(strict_types=1);

namespace Flow\ETL\Retry;

final class RetriesRecord implements \Countable
{
    /**
     * @var array<FailedRetry>
     */
    private array $attempts = [];

    public function add(FailedRetry $attempt) : void
    {
        $this->attempts[] = $attempt;
    }

    /**
     * @return array<FailedRetry>
     */
    public function attempts() : array
    {
        return $this->attempts;
    }

    public function count() : int
    {
        return \count($this->attempts);
    }

    public function last() : ?FailedRetry
    {
        if ($this->attempts === []) {
            return null;
        }

        return \end($this->attempts);
    }
}
