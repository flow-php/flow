<?php

declare(strict_types=1);

namespace Flow\ETL\Executor;

final class SourceRead
{
    private bool $narrowed = false;

    private int $open = 0;

    private int $rows = 0;

    public function opened(bool $narrowed): void
    {
        $this->open++;
        $this->narrowed = $this->narrowed || $narrowed;
    }

    public function counted(int $rows): void
    {
        $this->rows += $rows;
    }

    public function closed(): void
    {
        $this->open--;
    }

    public function rows(): int
    {
        return $this->rows;
    }

    /**
     * Every read ran to its end and none had a limit or a partition filter pushed into it.
     */
    public function isComplete(): bool
    {
        return $this->open === 0 && !$this->narrowed;
    }
}
