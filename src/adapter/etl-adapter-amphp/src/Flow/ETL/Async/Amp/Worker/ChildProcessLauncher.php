<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Amp\Worker;

use function Amp\async;
use Amp\Process\Process;
use Flow\ETL\Async\Socket\Worker\Pool;
use Flow\ETL\Async\Socket\Worker\WorkerLauncher;
use Psr\Log\LoggerInterface;

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

            async(fn () => $this->onStart(Process::start($path)));
        }
    }

    private function onStart(Process $process) : void
    {
        $stdout = $process->getStdout();
        $stderr = $process->getStderr();

        while (null !== $chunk = $stdout->read()) {
            foreach (\explode("\n", $chunk) as $chunkLine) {
                if ('' !== $chunkLine) {
                    $this->logger->debug($chunkLine);
                }
            }
        }

        while (null !== $chunk = $stderr->read()) {
            foreach (\explode("\n", $chunk) as $chunkLine) {
                if ('' !== $chunkLine) {
                    $this->logger->error($chunkLine);
                }
            }
        }
    }
}
