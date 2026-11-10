<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Heartbeat;

use DateTimeImmutable;
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

use function array_filter;
use function array_map;
use function array_values;
use function intdiv;
use function max;
use function min;

final class HeartbeatThrottle implements ProgressStages
{
    private int $batches = 0;

    private readonly Consumption $memory;

    private ?DateTimeImmutable $nextBeatAt = null;

    private int $othersMemory = 0;

    private bool $othersNewer = false;

    private int $othersPeak = 0;

    private int $othersRows = 0;

    private string $othersStage = '';

    private int $othersStageRows = 0;

    private bool $progressed = false;

    private DateTimeImmutable $progressedAt;

    private int $rowsRead = 0;

    private int $runs = 0;

    private string $stage = '';

    private int $stageAdvancedAt = -1;

    private int $stageRows = 0;

    private DateTimeImmutable $startedAt;

    private bool $stopped = false;

    private int $unreadRows = 0;

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

    public function othersReported(
        int $addedRows,
        string $stage,
        int $stageRows,
        int $addedMemoryBytes,
        int $peakMemoryBytes,
    ): void {
        $this->progressed =
            $this->progressed
            || $addedRows !== 0
            || $stageRows !== $this->othersStageRows
            || $stage !== $this->othersStage;
        $this->othersRows += $addedRows;
        $this->othersStage = $stage;
        $this->othersStageRows = $stageRows;
        $this->othersMemory += $addedMemoryBytes;
        $this->othersPeak = max($this->othersPeak, $peakMemoryBytes);
        $this->othersNewer = true;
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
                $this->progressed = $this->progressed || $batch->count() > 0;

                // a stage that advanced since the previous batch took it, so the label stays that stage
                if (
                    $this->batches === 0
                    || $this->stageAdvancedAt !== $this->batches
                    || $this->stage === ProgressStages::INFER
                ) {
                    $this->stage = ProgressStages::READ;
                    $this->stageRows = $this->rowsRead;
                    $this->othersNewer = false;
                }

                $this->batches++;
            }

            $signal = yield $batch;

            if ($signal === Signal::STOP) {
                $batches->send(Signal::STOP);

                return;
            }
        }
    }

    public function rowsRead(): int
    {
        return $this->rowsRead + $this->othersRows - $this->unreadRows;
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
        $this->stageAdvancedAt = $this->batches;
        $this->progressed = true;
        $this->beatWhenDue();
    }

    /**
     * An abandoned read's rows: whoever reads them again counts them.
     */
    public function unread(int $rows): void
    {
        $this->unreadRows += $rows;
    }

    public function started(): void
    {
        if ($this->runs++ === 0) {
            $this->start();
        }
    }

    public function stopped(): bool
    {
        return $this->stopped;
    }

    /**
     * @throws Throwable what a heartbeat failing with HeartbeatFailure::stop threw
     */
    public function beatWhenDue(): void
    {
        if ($this->stopped) {
            return;
        }

        $now = $this->clock->now();
        $this->settleProgress($now);

        if ($this->nextBeatAt === null || $now < $this->nextBeatAt) {
            return;
        }

        $this->beat(
            $now,
            array_values(array_filter(
                $this->heartbeats,
                static fn(ScheduledHeartbeat $heartbeat): bool => $heartbeat->nextBeatAt() <= $now,
            )),
        );
    }

    /**
     * @throws Throwable what a heartbeat failing with HeartbeatFailure::stop threw
     */
    public function beatNow(): void
    {
        if ($this->stopped) {
            return;
        }

        $now = $this->clock->now();
        $this->settleProgress($now);
        $this->beat($now, $this->heartbeats);
    }

    /**
     * @param list<ScheduledHeartbeat> $heartbeats
     *
     * @throws Throwable what a heartbeat failing with HeartbeatFailure::stop threw
     */
    private function beat(DateTimeImmutable $now, array $heartbeats): void
    {
        $elapsedSeconds = self::wholeSeconds($this->startedAt, $now);
        $idleSeconds = self::wholeSeconds($this->progressedAt, $now);
        $rowsRead = $this->rowsRead();
        $stage = $this->othersNewer ? $this->othersStage : $this->stage;
        $stageRows = $this->othersNewer ? $this->othersStageRows : $this->stageRows;
        $memoryBytes = $this->memory->capture()->inBytes() + $this->othersMemory;
        $peakMemoryBytes = max(Unit::processPeak()->inBytes(), $this->othersPeak);
        // a heartbeat that reports progress itself must not be called again inside its own beat
        $this->nextBeatAt = null;

        try {
            foreach ($heartbeats as $heartbeat) {
                $heartbeat->beat(
                    new Pulse(
                        $heartbeat->nextBeat(),
                        $elapsedSeconds,
                        $rowsRead,
                        $stage,
                        $stageRows,
                        $memoryBytes,
                        $peakMemoryBytes,
                        $idleSeconds,
                    ),
                    $this->clock,
                    $this->logger,
                );
            }
        } catch (Throwable $failure) {
            // ScheduledHeartbeat::beat() rethrows only under HeartbeatFailure::stop
            $this->stopped = true;

            throw $failure;
        } finally {
            $this->nextBeatAt = min(array_map(
                static fn(ScheduledHeartbeat $heartbeat): DateTimeImmutable => $heartbeat->nextBeatAt(),
                $this->heartbeats,
            ));
        }
    }

    private static function wholeSeconds(DateTimeImmutable $from, DateTimeImmutable $to): int
    {
        return intdiv((int) $to->format('Uu') - (int) $from->format('Uu'), 1_000_000);
    }

    private function settleProgress(DateTimeImmutable $now): void
    {
        if ($this->progressed) {
            $this->progressedAt = $now;
            $this->progressed = false;
        }
    }

    private function start(): void
    {
        $now = $this->clock->now();
        $this->startedAt = $now;
        $this->progressedAt = $now;
        $this->progressed = false;
        $this->batches = 0;
        $this->stageAdvancedAt = -1;
        $this->rowsRead = 0;
        $this->unreadRows = 0;
        $this->stage = '';
        $this->stageRows = 0;
        $this->othersRows = 0;
        $this->othersStageRows = 0;
        $this->othersStage = '';
        $this->othersMemory = 0;
        $this->othersPeak = 0;
        $this->othersNewer = false;
        $this->stopped = false;

        foreach ($this->heartbeats as $heartbeat) {
            $heartbeat->start($now);
        }

        $this->nextBeatAt = min(array_map(
            static fn(ScheduledHeartbeat $heartbeat): DateTimeImmutable => $heartbeat->nextBeatAt(),
            $this->heartbeats,
        ));
    }
}
