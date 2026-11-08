<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use ArrayObject;
use Flow\ETL\Foundation\Clock\FakeClock;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Runtime\Heartbeat\HeartbeatThrottle;
use Flow\ETL\Runtime\Heartbeat\Pulse;
use Flow\ETL\Runtime\Heartbeat\ScheduledHeartbeat;
use Flow\ETL\Tests\Double\RecordingHeartbeat;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Generator;

use function array_map;
use function array_values;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
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
     * @return list<array{int, int}> beat number and elapsed seconds of every pulse
     */
    public static function beats(RecordingHeartbeat $heartbeat): array
    {
        return array_map(static fn(Pulse $pulse): array => [
            $pulse->beat,
            $pulse->elapsedSeconds,
        ], $heartbeat->pulses());
    }

    public function errorLogs(): int
    {
        return count($this->logs->entriesWithSeverity(Severity::ERROR));
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

    public function throttle(ScheduledHeartbeat $first, ScheduledHeartbeat ...$more): HeartbeatThrottle
    {
        return new HeartbeatThrottle($this->clock, new PhpBackend(), $this->logger, [$first, ...array_values($more)]);
    }
}
