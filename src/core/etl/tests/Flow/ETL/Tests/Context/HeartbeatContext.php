<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use ArrayObject;
use Flow\ETL\Foundation\Clock\FakeClock;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\StageProgress;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Runtime\Heartbeat\HeartbeatStage;
use Flow\ETL\Runtime\Heartbeat\HeartbeatThrottle;
use Flow\ETL\Runtime\Heartbeat\Pulse;
use Flow\ETL\Runtime\Heartbeat\ScheduledHeartbeat;
use Flow\ETL\Tests\Double\RecordingHeartbeat;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Testing\FilesystemOperation;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Generator;

use function array_filter;
use function array_map;
use function array_values;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\intercepted_filesystem;
use function range;

final readonly class HeartbeatContext
{
    public FakeClock $clock;

    public Logger $logger;

    public MemoryLogProcessor $logs;

    public function __construct()
    {
        $this->clock = new FakeClock();
        $this->logs = new MemoryLogProcessor(new VoidExporter());
        $this->logger = (new LoggerProvider($this->logs, $this->clock, new MemoryContextStorage()))->logger(
            Resource::create(['service.name' => 'flow-test']),
            'flow_php_dataframe',
        );
    }

    /**
     * @return Generator<int, CorruptedRecordException|Rows>
     */
    public static function batches(int $count, ?CorruptedRecordException $afterFirst = null): Generator
    {
        foreach (range(1, $count) as $batch) {
            yield array_to_rows(
                array_map(static fn(int $id): array => ['id' => $id], range(1, 10)),
                schema(int_schema('id')),
            );

            if ($batch === 1 && $afterFirst !== null) {
                yield $afterFirst;
            }
        }
    }

    /**
     * @return list<array{int, float}> beat number and elapsed seconds of every pulse
     */
    public static function beats(RecordingHeartbeat $heartbeat): array
    {
        return array_map(static fn(Pulse $pulse): array => [
            $pulse->beat,
            $pulse->elapsedSeconds,
        ], $heartbeat->pulses());
    }

    /**
     * @return list<list<StageProgress>> the entries named $name of every pulse
     */
    public static function entries(RecordingHeartbeat $recording, StageName $name): array
    {
        return array_map(static fn(Pulse $pulse): array => array_values(array_filter(
            $pulse->stages,
            static fn(StageProgress $stage): bool => $stage->name === $name,
        )), $recording->pulses());
    }

    public function errorLogs(): int
    {
        return count($this->logs->entriesWithSeverity(Severity::ERROR));
    }

    public function slowMoves(Filesystem $filesystem, int $seconds = 10): Filesystem
    {
        return intercepted_filesystem($filesystem)->onCall(function (FilesystemOperation $operation, Path $path) use (
            $seconds,
        ): void {
            if ($operation === FilesystemOperation::Mv) {
                $this->clock->modify('+' . $seconds . ' seconds');
            }
        });
    }

    public function seconds(int $seconds): void
    {
        $this->clock->modify("+{$seconds} seconds");
    }

    /**
     * @param ArrayObject<int, Signal> $signals
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public static function stoppable(ArrayObject $signals): Generator
    {
        foreach (range(1, 3) as $_) {
            $signal = yield array_to_rows([['id' => 1]], schema(int_schema('id')));

            if ($signal !== null) {
                $signals->append($signal);

                return;
            }
        }
    }

    public static function stageWithId(StageName $name, int $id): HeartbeatStage
    {
        return (new self())
            ->throttle(new ScheduledHeartbeat(new RecordingHeartbeat(), 120))
            ->stage($name, ProgressUnit::rows, $id);
    }

    public function throttleToParent(ScheduledHeartbeat $heartbeat): HeartbeatThrottle
    {
        return HeartbeatThrottle::reportingToParent($this->clock, new PhpBackend(), $this->logger, [$heartbeat]);
    }

    public function throttle(ScheduledHeartbeat $first, ScheduledHeartbeat ...$more): HeartbeatThrottle
    {
        return new HeartbeatThrottle($this->clock, new PhpBackend(), $this->logger, [$first, ...array_values($more)]);
    }
}
