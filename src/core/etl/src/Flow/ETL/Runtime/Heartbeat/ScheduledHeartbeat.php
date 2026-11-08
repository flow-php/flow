<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Heartbeat;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\Telemetry\Logger\Logger;
use Psr\Clock\ClockInterface;
use Throwable;

final class ScheduledHeartbeat
{
    private int $beats = 0;

    private int $nextBeatAt = 0;

    /**
     * @throws InvalidArgumentException when $everySeconds is below 1
     */
    public function __construct(
        public readonly Heartbeat $heartbeat,
        public readonly int $everySeconds,
        public readonly HeartbeatFailure $onFailure = HeartbeatFailure::stop,
    ) {
        if ($everySeconds < 1) {
            throw InvalidArgumentException::because(
                'Heartbeat interval must be at least 1 second, got %d',
                $everySeconds,
            );
        }
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
            // read after the callback, so a slow heartbeat never fires again straight away
            $this->nextBeatAt = $clock->now()->getTimestamp() + $this->everySeconds;
        }
    }

    public function nextBeat(): int
    {
        return $this->beats + 1;
    }

    public function nextBeatAt(): int
    {
        return $this->nextBeatAt;
    }

    public function start(int $now): void
    {
        $this->beats = 0;
        $this->nextBeatAt = $now + $this->everySeconds;
    }
}
