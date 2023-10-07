<?php

declare(strict_types=1);

namespace Flow\ETL\Async\ReactPHP\Worker;

use Flow\ETL\Async\Socket\Worker\Pool;
use Flow\ETL\Async\Socket\Worker\WorkerLauncher;
use Psr\Log\LoggerInterface;
use React\ChildProcess\Process;

final class ChildProcessLauncher implements WorkerLauncher
{
    public function __construct(
        private readonly string $workerPath,
        private readonly LoggerInterface $logger
    ) {
    }

    public function launch(Pool $pool, string $host) : void
    {
        foreach ($pool->ids() as $id) {
            $path = \realpath($this->workerPath) . " --id=\"{$id}\" --host=\"{$host}\"";

            $this->logger->debug('starting worker', ['path' => $path]);

            $process = new Process(
                $path,
                null,
            );

            $process->start();

            $process->stderr->on('data', function ($chunk) : void {
                foreach (\explode("\n", $chunk) as $chunkLine) {
                    if ('' !== $chunkLine) {
                        $this->logger->error($chunkLine);
                    }
                }
            });

            $process->stdout->on('data', function ($chunk) : void {
                foreach (\explode("\n", $chunk) as $chunkLine) {
                    if ('' !== $chunkLine) {
                        $this->logger->debug($chunkLine);
                    }
                }
            });
        }
    }
}
