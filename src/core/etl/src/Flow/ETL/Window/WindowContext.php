<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\FlowContext;
use Flow\ETL\Rows;

final class WindowContext
{
    private ?Rows $frame = null;

    public function __construct(
        private readonly int $index,
        private readonly Rows $partition,
        private readonly WindowFrame $windowFrame,
        private readonly FlowContext $flowContext,
    ) {}

    public function flowContext(): FlowContext
    {
        return $this->flowContext;
    }

    /**
     * Resolved lazily - ranking functions ignore frames, so they never pay for a slice they discard.
     */
    public function frame(): Rows
    {
        if ($this->frame === null) {
            [$start, $end] = $this->windowFrame->bounds($this->index, $this->partition);

            $this->frame = $start > $end
                ? Rows::empty($this->partition->schema(), $this->flowContext->backend())
                : $this->partition->slice($start, $end - $start + 1);
        }

        return $this->frame;
    }

    public function index(): int
    {
        return $this->index;
    }

    public function partition(): Rows
    {
        return $this->partition;
    }
}
