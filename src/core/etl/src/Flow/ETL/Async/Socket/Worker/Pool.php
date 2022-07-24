<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Worker;

use Flow\ETL\Async\Socket\Worker\Pool\Worker;
use Flow\ETL\Async\Socket\Worker\Pool\WorkerStatus;

final class Pool implements \Countable
{
    /**
     * @var array<string, Worker>
     */
    private array $workers;

    /**
     * @param array<Worker> $workers
     */
    private function __construct(array $workers)
    {
        $this->workers = [];

        foreach ($workers as $worker) {
            $this->workers[$worker->id()] = $worker;
        }
    }

    public static function generate(int $size) : self
    {
        /** @psalm-suppress UnusedClosureParam */
        return new self(
            \array_map(fn (int $i) => new Worker(\uniqid('worker_' . $i, true)), \range(1, $size))
        );
    }

    public function connect(string $id) : void
    {
        $this->workers[$id]->connect();
    }

    public function count() : int
    {
        return \count($this->workers);
    }

    public function disconnect(string $id) : void
    {
        $this->workers[$id]->disconnect();
    }

    public function has(string $id) : bool
    {
        return \array_key_exists($id, $this->workers);
    }

    /**
     * @return array<string>
     */
    public function ids() : array
    {
        return \array_keys($this->workers);
    }

    public function onlyConnected() : self
    {
        $workers = [];

        foreach ($this->workers as $worker) {
            if ($worker->status() === WorkerStatus::connected) {
                $workers[] = $worker;
            }
        }

        return new self($workers);
    }
}
