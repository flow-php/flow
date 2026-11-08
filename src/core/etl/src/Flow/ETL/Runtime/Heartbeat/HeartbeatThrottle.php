<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Heartbeat;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\ProgressStages;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\Consumption;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\Rows;
use Flow\Telemetry\Logger\Logger;
use Generator;
use Psr\Clock\ClockInterface;
use Throwable;

use function array_map;
use function max;
use function min;

use const PHP_INT_MAX;

final class HeartbeatThrottle implements ProgressStages
{
    private readonly Consumption $memory;

    private int $nextBeatAt = 0;

    private int $othersMemory = 0;

    private bool $othersNewer = false;

    private int $othersPeak = 0;

    private int $othersRows = 0;

    private string $othersStage = '';

    private int $othersStageRows = 0;

    private int $rowsRead = 0;

    private int $runs = 0;

    private string $stage = '';

    private int $stageRows = 0;

    private int $startedAt = 0;

    /**
     * @param non-empty-list<ScheduledHeartbeat> $heartbeats called in this order when due together
     */
    public function __construct(
        private readonly ClockInterface $clock,
        Backend $backend,
        private readonly Logger $logger,
        private readonly array $heartbeats,
    ) {
        $this->memory = new Consumption($backend);
        // a worker never calls started(), so the throttle runs from its creation
        $this->start();
    }

    public function finished(): void
    {
        $this->runs--;
    }

    /**
     * @throws Throwable what a heartbeat failing with HeartbeatFailure::stop threw
     */
    public function othersReported(
        int $rows,
        string $stage,
        int $stageRows,
        int $memoryBytes,
        int $peakMemoryBytes,
    ): void {
        $this->othersRows = $rows;
        $this->othersStage = $stage;
        $this->othersStageRows = $stageRows;
        $this->othersMemory = $memoryBytes;
        $this->othersPeak = $peakMemoryBytes;
        $this->othersNewer = true;
        $this->beatWhenDue();
    }

    /**
     * Counts the rows of $batches as read from the source, a corrupted record none, and forwards a STOP; the reader
     * of the batches checks for a due beat with beatWhenDue().
     *
     * @param Generator<CorruptedRecordException|Rows> $batches
     *
     * @return Generator<int, CorruptedRecordException|Rows, null|Signal, void>
     */
    public function reading(Generator $batches): Generator
    {
        foreach ($batches as $batch) {
            if ($batch instanceof Rows) {
                $this->rowsRead += $batch->count();
                $this->stage = ProgressStages::READ;
                $this->stageRows = $this->rowsRead;
                $this->othersNewer = false;
            }

            $signal = yield $batch;

            if ($signal === Signal::STOP) {
                $batches->send(Signal::STOP);

                return;
            }
        }
    }

    public function stage(string $name): Progress
    {
        return new HeartbeatStage($name, $this);
    }

    /**
     * @throws Throwable what a heartbeat failing with HeartbeatFailure::stop threw
     */
    public function stageAdvanced(string $stage, int $stageRows): void
    {
        $this->stage = $stage;
        $this->stageRows = $stageRows;
        $this->othersNewer = false;
        $this->beatWhenDue();
    }

    public function started(): void
    {
        if ($this->runs++ === 0) {
            $this->start();
        }
    }

    /**
     * @throws Throwable what a heartbeat failing with HeartbeatFailure::stop threw
     */
    public function beatWhenDue(): void
    {
        $now = $this->clock->now()->getTimestamp();

        if ($now < $this->nextBeatAt) {
            return;
        }

        $elapsedSeconds = $now - $this->startedAt;
        $rowsRead = $this->rowsRead + $this->othersRows;
        $stage = $this->othersNewer ? $this->othersStage : $this->stage;
        $stageRows = $this->othersNewer ? $this->othersStageRows : $this->stageRows;
        $memoryBytes = $this->memory->capture()->inBytes() + $this->othersMemory;
        $peakMemoryBytes = max(Unit::processPeak()->inBytes(), $this->othersPeak);
        // a heartbeat that reports progress itself must not be called again inside its own beat
        $this->nextBeatAt = PHP_INT_MAX;

        try {
            foreach ($this->heartbeats as $heartbeat) {
                if ($heartbeat->nextBeatAt() <= $now) {
                    $heartbeat->beat(
                        new Pulse(
                            $heartbeat->nextBeat(),
                            $elapsedSeconds,
                            $rowsRead,
                            $stage,
                            $stageRows,
                            $memoryBytes,
                            $peakMemoryBytes,
                        ),
                        $this->clock,
                        $this->logger,
                    );
                }
            }
        } finally {
            $this->nextBeatAt = min(array_map(
                static fn(ScheduledHeartbeat $heartbeat): int => $heartbeat->nextBeatAt(),
                $this->heartbeats,
            ));
        }
    }

    private function start(): void
    {
        $now = $this->clock->now()->getTimestamp();
        $this->startedAt = $now;
        $this->rowsRead = 0;
        $this->stage = '';
        $this->stageRows = 0;
        $this->othersRows = 0;
        $this->othersStageRows = 0;
        $this->othersStage = '';
        $this->othersMemory = 0;
        $this->othersPeak = 0;
        $this->othersNewer = false;

        foreach ($this->heartbeats as $heartbeat) {
            $heartbeat->start($now);
        }

        $this->nextBeatAt =
            $now
            + min(array_map(
                static fn(ScheduledHeartbeat $heartbeat): int => $heartbeat->everySeconds,
                $this->heartbeats,
            ));
    }
}
