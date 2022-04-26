<?php declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Worker;

interface WorkerLauncher
{
    public function launch(Pool $pool, string $host) : void;
}
