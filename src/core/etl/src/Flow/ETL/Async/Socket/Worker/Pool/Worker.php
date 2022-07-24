<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Worker\Pool;

final class Worker
{
    private WorkerStatus $status;

    public function __construct(private readonly string $id)
    {
        $this->status = WorkerStatus::new;
    }

    public function connect() : void
    {
        $this->status = WorkerStatus::connected;
    }

    public function disconnect() : void
    {
        $this->status = WorkerStatus::disconnected;
    }

    public function id() : string
    {
        return $this->id;
    }

    public function status() : WorkerStatus
    {
        return $this->status;
    }
}
