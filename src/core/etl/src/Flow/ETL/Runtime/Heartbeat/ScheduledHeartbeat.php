<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Heartbeat;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\Telemetry\Logger\Logger;
use Psr\Clock\ClockInterface;
use Throwable;

use function round;

final class ScheduledHeartbeat
{
    private int $beats = 0;

    private readonly DateInterval $interval;

    private DateTimeImmutable $nextBeatAt;

    /**
     * @throws InvalidArgumentException when $everySeconds is below 0.1
     */
    public function __construct(
        public readonly Heartbeat $heartbeat,
        public readonly float $everySeconds,
        public readonly HeartbeatFailure $onFailure = HeartbeatFailure::stop,
    ) {
        if ($everySeconds < 0.1) {
            throw InvalidArgumentException::because(
                'Heartbeat interval must be at least 0.1 seconds, got %s',
                $everySeconds,
            );
        }

        $this->interval = DateInterval::createFromDateString((int) round($everySeconds * 1_000_000) . ' microseconds');
        $this->nextBeatAt = new DateTimeImmutable('@0');
    }

    /**
     * @throws Throwable what the heartbeat threw, when it fails with HeartbeatFailure::stop
     */
    public function beat(Pulse $pulse, ClockInterface $clock, Logger $logger): void
    {
        $this->beats++;

        try {
            $this->heartbeat->beat($pulse);
        } catch (Throwable $failure) {
            if ($this->onFailure === HeartbeatFailure::stop) {
                throw $failure;
            }

            $logger->error('Heartbeat failed.', ['exception' => $failure]);
        } finally {
            // on the interval grid; a beat called early or ending past its next due time restarts from now
            $now = $clock->now();
            $onGrid = $this->nextBeatAt->add($this->interval);
            $this->nextBeatAt = $this->nextBeatAt <= $now && $onGrid > $now ? $onGrid : $now->add($this->interval);
        }
    }

    public function nextBeat(): int
    {
        return $this->beats + 1;
    }

    public function nextBeatAt(): DateTimeImmutable
    {
        return $this->nextBeatAt;
    }

    public function start(DateTimeImmutable $now): void
    {
        $this->beats = 0;
        $this->nextBeatAt = $now->add($this->interval);
    }
}
