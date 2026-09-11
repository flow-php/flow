<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Plan;

/**
 * @type BuffersShape = array{shared_hit: int, shared_read: int, shared_dirtied: int, shared_written: int, local_hit: int, local_read: int, local_dirtied: int, local_written: int, temp_read: int, temp_written: int}
 */
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
    ) {}

    /**
     * @param BuffersShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            sharedHit: $data['shared_hit'],
            sharedRead: $data['shared_read'],
            sharedDirtied: $data['shared_dirtied'],
            sharedWritten: $data['shared_written'],
            localHit: $data['local_hit'],
            localRead: $data['local_read'],
            localDirtied: $data['local_dirtied'],
            localWritten: $data['local_written'],
            tempRead: $data['temp_read'],
            tempWritten: $data['temp_written'],
        );
    }

    public function hasDiskSpill(): bool
    {
        return $this->tempRead > 0 || $this->tempWritten > 0;
    }

    public function hitRatio(): float
    {
        $total = $this->totalSharedBlocks();

        return $total > 0 ? $this->sharedHit / $total : 1.0;
    }

    public function localDirtied(): int
    {
        return $this->localDirtied;
    }

    public function localHit(): int
    {
        return $this->localHit;
    }

    public function localRead(): int
    {
        return $this->localRead;
    }

    public function localWritten(): int
    {
        return $this->localWritten;
    }

    /**
     * @return BuffersShape
     */
    public function normalize(): array
    {
        return [
            'shared_hit' => $this->sharedHit,
            'shared_read' => $this->sharedRead,
            'shared_dirtied' => $this->sharedDirtied,
            'shared_written' => $this->sharedWritten,
            'local_hit' => $this->localHit,
            'local_read' => $this->localRead,
            'local_dirtied' => $this->localDirtied,
            'local_written' => $this->localWritten,
            'temp_read' => $this->tempRead,
            'temp_written' => $this->tempWritten,
        ];
    }

    public function sharedDirtied(): int
    {
        return $this->sharedDirtied;
    }

    public function sharedHit(): int
    {
        return $this->sharedHit;
    }

    public function sharedRead(): int
    {
        return $this->sharedRead;
    }

    public function sharedWritten(): int
    {
        return $this->sharedWritten;
    }

    public function tempBlocks(): int
    {
        return $this->tempRead + $this->tempWritten;
    }

    public function tempRead(): int
    {
        return $this->tempRead;
    }

    public function tempWritten(): int
    {
        return $this->tempWritten;
    }

    public function totalSharedBlocks(): int
    {
        return $this->sharedHit + $this->sharedRead;
    }
}
