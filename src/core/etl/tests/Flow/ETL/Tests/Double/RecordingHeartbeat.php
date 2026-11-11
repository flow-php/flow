<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use ArrayObject;
use Flow\ETL\Runtime\Heartbeat\Heartbeat;
use Flow\ETL\Runtime\Heartbeat\Pulse;
use Throwable;

final class RecordingHeartbeat implements Heartbeat
{
    /**
     * @var list<Pulse>
     */
    private array $pulses = [];

    /**
     * @param ArrayObject<int, array{string, int}> $calls name and beat of every call, shareable between heartbeats
     */
    public function __construct(
        private readonly string $name = '',
        private readonly ?Throwable $throws = null,
        public readonly ArrayObject $calls = new ArrayObject(),
    ) {}

    public function beat(Pulse $pulse): void
    {
        $this->pulses[] = $pulse;
        $this->calls->append([$this->name, $pulse->beat]);

        if ($this->throws !== null) {
            throw $this->throws;
        }
    }

    /**
     * @return list<Pulse>
     */
    public function pulses(): array
    {
        return $this->pulses;
    }
}
