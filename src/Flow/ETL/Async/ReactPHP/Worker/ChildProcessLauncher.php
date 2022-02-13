<?php

declare(strict_types=1);

namespace Flow\ETL\Async\ReactPHP\Worker;

use Flow\ETL\Async\Client\Launcher;
use Flow\ETL\Async\Client\Pool;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use React\ChildProcess\Process;

final class ChildProcessLauncher implements Launcher
{
    private string $workerPath;

    /**
     * @var LoggerInterface
     */
    private LoggerInterface $logger;

    private int $port;

    public function __construct(string $workerPath, int $port, LoggerInterface $logger)
    {
        $this->workerPath = $workerPath;
        $this->logger = $logger;
        $this->port = $port;
    }

    public function launch(Pool $pool) : void
    {
        foreach ($pool->ids() as $id) {
            $stringId = $id->toString();

            $path = \realpath($this->workerPath) . " --id=\"{$stringId}\" --host=\"127.0.0.1\" --port=\"{$this->port}\"";

            $this->logger->log(
                LogLevel::DEBUG,
                '[worker launcher] starting worker',
                ['path' => $path]
            );

            $process = new Process(
                $path,
                null,
            );

            $process->start();

            $process->stderr->on('data', function ($chunk) : void {
                foreach (\explode("\n", $chunk) as $chunkLine) {
                    $this->logger->error($chunkLine);
                }
            });
        }
    }
}
