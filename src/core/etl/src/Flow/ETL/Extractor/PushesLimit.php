<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;

use function min;

trait PushesLimit
{
    private ?int $pushedLimit = null;

    public function pushLimit(int $limit): void
    {
        if ($limit <= 0) {
            throw new InvalidArgumentException('Limit must be greater than 0');
        }

        $this->pushedLimit = $this->pushedLimit === null ? $limit : min($this->pushedLimit, $limit);
    }

    public function pushedLimit(): ?int
    {
        return $this->pushedLimit;
    }
}
