<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Heartbeat;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\ProgressStages;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\StageProgress;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\Consumption;
use Flow\ETL\Values\Rows;
use Flow\Telemetry\Logger\Logger;
use Generator;
use Psr\Clock\ClockInterface;
use Throwable;

use function array_filter;
use function array_key_exists;
use function array_keys;
use function array_map;
use function array_reverse;
use function array_sum;
use function array_values;
use function max;
use function memory_get_peak_usage;
use function min;
use function usort;

/**
 * @type OwnStage = array{id: ?int, name: StageName, unit: ProgressUnit, subject: ?string, count: int, total: ?int, startedAt: ?float, endedAt: ?float, open: int}
 * @type ReportedTimes = array{startedAt: float, endedAt: ?float}
 */
final class HeartbeatThrottle implements ProgressStages
{
    /**
     * @var int the PHP peak when the run started: a higher peak was reached by this run
     */
    private int $heapMark = 0;

    /**
     * @var array<string, OwnStage> stages made since started() that are not listed yet, by key
     */
    private array $made = [];

    private readonly Consumption $memory;

    private ?DateTimeImmutable $nextBeatAt = null;

    private int $objects = 0;

    /**
     * @var array<string, true> every key in the order this process first saw it, own or reported
     */
    private array $order = [];

    private int $othersMemory = 0;

    private int $othersPeak = 0;

    /**
     * @var int the highest memory of this process and the reporting ones together, sampled whenever the total changes
     */
    private int $totalPeak = 0;

    private int $othersRows = 0;

    private bool $progressed = false;

    private DateTimeImmutable $progressedAt;

    /**
     * @var array<string, array<int, true>> the objects that reported each key, by key
     */
    private array $reporters = [];

    /**
     * @var array<string, ReportedTimes> when this process saw a reported entry start and end, by key
     */
    private array $reportedTimes = [];

    /**
     * @var array<int, array<string, non-empty-list<StageProgress>>> the latest parts of each reporting object by key
     */
    private array $reports = [];

    private int $rowsRead = 0;

    private bool $reportsToParent = false;

    private int $runs = 0;

    /**
     * @var array<string, OwnStage> own entries in the order they opened, by key
     */
    private array $stages = [];

    private DateTimeImmutable $startedAt;

    private bool $stopped = false;

    private int $unreadRows = 0;

    /**
     * @param non-empty-list<ScheduledHeartbeat> $heartbeats called in this order when due together
     */
    public function __construct(
        private readonly ClockInterface $clock,
        private readonly Backend $backend,
        private readonly Logger $logger,
        private readonly array $heartbeats,
    ) {
        $this->memory = new Consumption($backend);
        // a worker never calls started(), so the throttle runs from its creation
        $this->start();
    }

    /**
     * A run that a worker step starts adds to what the worker counted, because the parent folds the worker's totals.
     *
     * @param non-empty-list<ScheduledHeartbeat> $heartbeats
     */
    public static function reportingToParent(
        ClockInterface $clock,
        Backend $backend,
        Logger $logger,
        array $heartbeats,
    ): self {
        $throttle = new self($clock, $backend, $logger, $heartbeats);
        $throttle->reportsToParent = true;

        return $throttle;
    }

    /**
     * @throws Throwable what a heartbeat failing with HeartbeatFailure::stop threw
     */
    public function completed(): void
    {
        if ($this->runs !== 1 || $this->reportsToParent) {
            return;
        }

        $now = $this->secondsSinceStart();

        foreach ($this->stages as $key => $stage) {
            if ($stage['open'] > 0 || $stage['startedAt'] === null && $stage['endedAt'] === null) {
                $this->stages[$key]['open'] = 0;
                $this->stages[$key]['endedAt'] = $now;
            }
        }

        $this->beatNow();
    }

    public function everySeconds(): float
    {
        return min(array_map(
            static fn(ScheduledHeartbeat $heartbeat): float => $heartbeat->everySeconds,
            $this->heartbeats,
        ));
    }

    public function finished(): void
    {
        $this->runs--;
    }

    /**
     * @param list<StageProgress> $stages
     */
    public function othersReported(
        int $object,
        int $addedRows,
        array $stages,
        int $addedMemoryBytes,
        int $peakMemoryBytes,
        bool $progressed,
    ): void {
        $parts = self::byKey($stages);
        $this->progressed =
            $this->progressed
            || $progressed
            || $addedRows !== 0
            || self::counts($this->reports[$object] ?? []) !== self::counts($parts);
        $this->othersRows += $addedRows;
        $this->othersMemory += $addedMemoryBytes;
        $this->othersPeak = max($this->othersPeak, $peakMemoryBytes);
        $this->totalPeak = max($this->totalPeak, $this->memory->capture()->inBytes() + $this->othersMemory);
        $this->reports[$object] = $parts;
        $now = $this->secondsSinceStart();

        foreach ($parts as $key => $keyParts) {
            $firstSeen = !array_key_exists($key, $this->reportedTimes);
            $newReporter = !array_key_exists($object, $this->reporters[$key] ?? []);
            $this->reporters[$key][$object] = true;

            if ($firstSeen) {
                $this->order[$key] = true;
                $this->reportedTimes[$key] = ['startedAt' => $now, 'endedAt' => null];
            }

            if (!$this->reportedEnded($key)) {
                $this->reportedTimes[$key]['endedAt'] = null;
            } elseif ($firstSeen || $newReporter) {
                // a stage that ran inside one report keeps the duration it reports, and a new launch moves the end
                $this->reportedTimes[$key] = [
                    'startedAt' => min($this->reportedTimes[$key]['startedAt'], $now - self::longestRun($keyParts)),
                    'endedAt' => $now,
                ];
            } else {
                $this->reportedTimes[$key]['endedAt'] ??= $now;
            }
        }
    }

    /**
     * No beat here: the reader of the batches beats, outside the source's failure path.
     *
     * @param Generator<CorruptedRecordException|Rows> $batches
     *
     * @return Generator<int, CorruptedRecordException|Rows, null|Signal, void>
     */
    public function reading(Generator $batches, Progress $read): Generator
    {
        $read = $read instanceof HeartbeatStage ? $read : $this->stage(StageName::read);

        try {
            foreach ($batches as $batch) {
                if ($batch instanceof Rows) {
                    $read->read($batch->count());
                }

                $signal = yield $batch;

                if ($signal === Signal::STOP) {
                    $batches->send(Signal::STOP);

                    return;
                }
            }
        } finally {
            $read->done();
        }
    }

    /**
     * @param null|int $total the exact rows of the source, null when it has none or the read is narrowed
     */
    public function readStage(?Progress $planned, Extractor $source, ?int $total): HeartbeatStage
    {
        // a pipeline planned without the run's heartbeat reads into a stage of its own
        return $planned instanceof HeartbeatStage
            ? $planned
            : $this->stage(
                StageName::read,
                subject: $source instanceof FileExtractor ? $source->source()->location() : null,
                total: $total,
            );
    }

    public function rowsRead(): int
    {
        return $this->rowsRead + $this->othersRows - $this->unreadRows;
    }

    public function stage(
        StageName $stage,
        ProgressUnit $unit = ProgressUnit::rows,
        ?int $id = null,
        ?string $subject = null,
        ?int $total = null,
    ): HeartbeatStage {
        $object = $this->objects++;
        $id ??= $this->assignsIds() ? $object : null;

        if ($this->assignsIds()) {
            $this->made[$id === null ? 'object:' . $object : 'id:' . $id] = [
                'id' => $id,
                'name' => $stage,
                'unit' => $unit,
                'subject' => $subject,
                'count' => 0,
                'total' => $total,
                'startedAt' => null,
                'endedAt' => null,
                'open' => 0,
            ];
        }

        return new HeartbeatStage($stage, $unit, $id, $subject, $this, $object, $total);
    }

    /**
     * @throws Throwable what a heartbeat failing with HeartbeatFailure::stop threw
     */
    public function stageAdvanced(string $key, int $count): void
    {
        // an object that opened before a new run started reports into an entry the new run no longer has
        if (array_key_exists($key, $this->stages)) {
            $this->stages[$key]['count'] += $count;
        }

        $this->progressed = $this->progressed || $count > 0;
        $this->beatWhenDue();
    }

    /**
     * Rows read from the source: counted, never a beat, because the reader beats outside the source's failure path.
     */
    public function stageRead(string $key, int $rows): void
    {
        $this->rowsRead += $rows;

        if (array_key_exists($key, $this->stages)) {
            $this->stages[$key]['count'] += $rows;
        }

        $this->progressed = $this->progressed || $rows > 0;
    }

    public function stageCountsIn(string $key, ProgressUnit $unit): void
    {
        if (array_key_exists($key, $this->stages) && $this->stages[$key]['unit'] !== $unit) {
            $this->stages[$key]['unit'] = $unit;
            $this->stages[$key]['count'] = 0;
        }
    }

    public function stageClosed(string $key): void
    {
        if (array_key_exists($key, $this->made)) {
            unset($this->made[$key]);

            return;
        }

        // completed() closed the entry already, or a new run started after the object opened
        if (($this->stages[$key]['open'] ?? 0) === 0) {
            return;
        }

        $this->stages[$key]['open']--;

        if ($this->stages[$key]['open'] === 0) {
            $this->stages[$key]['endedAt'] = $this->secondsSinceStart();
        }
    }

    public function stageOpened(
        string $key,
        ?int $id,
        StageName $name,
        ProgressUnit $unit,
        ?string $subject,
        ?int $total,
    ): void {
        unset($this->made[$key]);

        if (!array_key_exists($key, $this->stages)) {
            $this->order[$key] = true;
            $this->stages[$key] = [
                'id' => $id,
                'name' => $name,
                'unit' => $unit,
                'subject' => $subject,
                'count' => 0,
                'total' => $total,
                'startedAt' => $this->secondsSinceStart(),
                'endedAt' => null,
                'open' => 1,
            ];

            return;
        }

        $this->stages[$key]['startedAt'] ??= $this->secondsSinceStart();
        $this->stages[$key]['endedAt'] = null;
        $this->stages[$key]['open']++;
    }

    public function planned(): void
    {
        foreach ($this->made as $key => $entry) {
            $this->order[$key] = true;
            $this->stages[$key] = $entry;
        }

        $this->made = [];
    }

    /**
     * An abandoned read's rows: whoever reads them again counts them.
     */
    public function unread(int $rows): void
    {
        $this->unreadRows += $rows;

        foreach (array_reverse(array_keys($this->stages)) as $key) {
            if ($this->stages[$key]['name'] === StageName::read) {
                $this->stages[$key]['count'] -= $rows;

                return;
            }
        }
    }

    public function started(): void
    {
        if ($this->runs++ === 0 && !$this->reportsToParent) {
            $this->start();
        }
    }

    public function stopped(): bool
    {
        return $this->stopped;
    }

    /**
     * This process is working, also where no stage counts: progress for idleSeconds, and a beat when one is due.
     *
     * @throws Throwable what a heartbeat failing with HeartbeatFailure::stop threw
     */
    public function busy(): void
    {
        $this->progressed = true;
        $this->beatWhenDue();
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
     * @param list<StageProgress> $stages
     *
     * @return array<string, non-empty-list<StageProgress>>
     */
    private static function byKey(array $stages): array
    {
        $parts = [];

        // a part without an id is a worker's own stage; only the parent's stages carry ids
        foreach ($stages as $stage) {
            if ($stage->id !== null) {
                $parts['id:' . $stage->id][] = $stage;
            }
        }

        return $parts;
    }

    /**
     * @param array<string, non-empty-list<StageProgress>> $parts
     *
     * @return array<string, int>
     */
    private static function counts(array $parts): array
    {
        return array_map(static fn(array $keyParts): int => array_sum(array_map(
            static fn(StageProgress $stage): int => $stage->count,
            $keyParts,
        )), $parts);
    }

    /**
     * @param non-empty-list<StageProgress> $parts
     */
    private static function longestRun(array $parts): float
    {
        return max(array_map(static fn(StageProgress $stage): float => $stage->startedAtSeconds === null
            ? 0.0
            : ($stage->endedAtSeconds ?? $stage->startedAtSeconds) - $stage->startedAtSeconds, $parts));
    }

    private static function seconds(DateTimeImmutable $from, DateTimeImmutable $to): float
    {
        return ((int) $to->format('Uu') - (int) $from->format('Uu')) / 1_000_000;
    }

    /**
     * @param list<ScheduledHeartbeat> $heartbeats
     *
     * @throws Throwable what a heartbeat failing with HeartbeatFailure::stop threw
     */
    private function beat(DateTimeImmutable $now, array $heartbeats): void
    {
        $elapsedSeconds = self::seconds($this->startedAt, $now);
        $idleSeconds = self::seconds($this->progressedAt, $now);
        $rowsRead = $this->rowsRead();
        $stages = $this->stageList();
        $memoryBytes = $this->memory->capture()->inBytes() + $this->othersMemory;
        $this->totalPeak = max($this->totalPeak, $memoryBytes);
        $heapPeak = memory_get_peak_usage(true);
        // a reporter's own peak is a true lower bound of the total at its moment, and it catches a spike between reports
        $peakMemoryBytes = max(
            $this->totalPeak,
            $this->othersPeak,
            $heapPeak > $this->heapMark ? $heapPeak : 0,
            $this->backend->allocatedPeakBytes(),
        );
        // a heartbeat that reports progress itself must not be called again inside its own beat
        $this->nextBeatAt = null;

        try {
            foreach ($heartbeats as $heartbeat) {
                $heartbeat->beat(
                    new Pulse(
                        $heartbeat->nextBeat(),
                        $elapsedSeconds,
                        $rowsRead,
                        $stages,
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

    private function reportedEnded(string $key): bool
    {
        foreach ($this->reports as $parts) {
            foreach ($parts[$key] ?? [] as $stage) {
                if ($stage->endedAtSeconds === null) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Only a parent's own run numbers its stages: a worker's ids are the parent's, never its own object numbers.
     */
    private function assignsIds(): bool
    {
        return $this->runs > 0 && !$this->reportsToParent;
    }

    private function secondsSinceStart(): float
    {
        return self::seconds($this->startedAt, $this->clock->now());
    }

    private function settleProgress(DateTimeImmutable $now): void
    {
        if ($this->progressed) {
            $this->progressedAt = $now;
            $this->progressed = false;
        }
    }

    /**
     * @return list<StageProgress> one entry per key of $order: the own entry and every reported entry of the key
     */
    private function stageList(): array
    {
        $reported = [];

        foreach ($this->reports as $parts) {
            foreach ($parts as $key => $keyParts) {
                $reported[$key] = [...($reported[$key] ?? []), ...$keyParts];
            }
        }

        $list = [];

        foreach (array_keys($this->order) as $key) {
            $own = $this->stages[$key] ?? null;
            $others = $reported[$key] ?? [];
            $times = $this->reportedTimes[$key] ?? null;

            if ($own !== null) {
                [$id, $name, $subject, $total] = [$own['id'], $own['name'], $own['subject'], $own['total']];
                $startedAt = $own['startedAt'] === null
                    ? $times['startedAt'] ?? null
                    : min($own['startedAt'], $times['startedAt'] ?? $own['startedAt']);
                $endedAt = $own['startedAt'] === null && $times !== null ? $times['endedAt'] : $own['endedAt'];
                $unit = $own['unit'];
            } elseif ($others !== [] && $times !== null) {
                [$id, $name, $subject, $total] = [$others[0]->id, $others[0]->name, $others[0]->subject, null];
                $startedAt = $times['startedAt'];
                $endedAt = 0.0;
                $unit = ProgressUnit::rows;
            } else {
                continue;
            }

            // a part that switched to the stage's final unit sets the entry's unit; parts still in rows add nothing
            if ($unit === ProgressUnit::rows) {
                foreach ($others as $stage) {
                    $unit = $stage->unit === ProgressUnit::rows ? $unit : $stage->unit;
                }
            }

            $count = $own !== null && $own['unit'] === $unit ? $own['count'] : 0;

            // a reported part adds its count and never its total: the total is the parent's own exact one, or none
            foreach ($others as $stage) {
                if ($stage->unit === $unit) {
                    $count += $stage->count;
                }
            }

            if ($times !== null) {
                $endedAt = $endedAt === null || $times['endedAt'] === null ? null : max($endedAt, $times['endedAt']);
            }

            $list[] = new StageProgress($id, $name, $unit, $subject, $count, $total, $startedAt, $endedAt);
        }

        // the planner numbers stages in step order, so ids give the pipeline order; an entry without an id keeps its place
        $positions = [];
        $numbered = [];

        foreach ($list as $position => $stage) {
            if ($stage->id !== null) {
                $positions[] = $position;
                $numbered[] = $stage;
            }
        }

        usort(
            $numbered,
            static fn(StageProgress $left, StageProgress $right): int => (int) $left->id <=> (int) $right->id,
        );

        foreach ($positions as $index => $position) {
            $list[$position] = $numbered[$index];
        }

        return $list;
    }

    private function start(): void
    {
        $now = $this->clock->now();
        $this->startedAt = $now;
        $this->progressedAt = $now;
        $this->progressed = false;
        $this->rowsRead = 0;
        $this->unreadRows = 0;
        $this->othersRows = 0;
        $this->othersMemory = 0;
        $this->othersPeak = 0;
        $this->totalPeak = 0;
        $this->heapMark = memory_get_peak_usage(true);
        $this->backend->resetAllocatedPeak();
        $this->stopped = false;
        $this->stages = [];
        $this->made = [];
        $this->reports = [];
        $this->reporters = [];
        $this->reportedTimes = [];
        $this->order = [];

        foreach ($this->heartbeats as $heartbeat) {
            $heartbeat->start($now);
        }

        $this->nextBeatAt = min(array_map(
            static fn(ScheduledHeartbeat $heartbeat): DateTimeImmutable => $heartbeat->nextBeatAt(),
            $this->heartbeats,
        ));
    }
}
