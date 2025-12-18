<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Plan;

final readonly class Buffers
{
    public function __construct(
        private int $sharedHit,
        private int $sharedRead,
        private int $sharedDirtied,
        private int $sharedWritten,
        private int $localHit,
        private int $localRead,
        private int $localDirtied,
        private int $localWritten,
        private int $tempRead,
        private int $tempWritten,
    ) {
    }

    public function hasDiskSpill() : bool
    {
        return $this->tempRead > 0 || $this->tempWritten > 0;
    }

    public function hitRatio() : float
    {
        $total = $this->totalSharedBlocks();

        return $total > 0 ? $this->sharedHit / $total : 1.0;
    }

    public function localDirtied() : int
    {
        return $this->localDirtied;
    }

    public function localHit() : int
    {
        return $this->localHit;
    }

    public function localRead() : int
    {
        return $this->localRead;
    }

    public function localWritten() : int
    {
        return $this->localWritten;
    }

    public function sharedDirtied() : int
    {
        return $this->sharedDirtied;
    }

    public function sharedHit() : int
    {
        return $this->sharedHit;
    }

    public function sharedRead() : int
    {
        return $this->sharedRead;
    }

    public function sharedWritten() : int
    {
        return $this->sharedWritten;
    }

    public function tempBlocks() : int
    {
        return $this->tempRead + $this->tempWritten;
    }

    public function tempRead() : int
    {
        return $this->tempRead;
    }

    public function tempWritten() : int
    {
        return $this->tempWritten;
    }

    public function totalSharedBlocks() : int
    {
        return $this->sharedHit + $this->sharedRead;
    }
}
