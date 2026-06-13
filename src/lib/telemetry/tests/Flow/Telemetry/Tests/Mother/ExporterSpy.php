<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Signal\Signals;

use function count;

final class ExporterSpy implements Exporter
{
    /** @var list<Signals> */
    private array $exported = [];

    private int $shutdownCount = 0;

    public function __construct(
        private readonly bool $result = true,
    ) {}

    public function export(Signals $signal): bool
    {
        $this->exported[] = $signal;

        return $this->result;
    }

    public function shutdown(): void
    {
        $this->shutdownCount++;
    }

    /**
     * @return list<Signals>
     */
    public function exported(): array
    {
        return $this->exported;
    }

    public function exportedCount(): int
    {
        return count($this->exported);
    }

    public function shutdownCount(): int
    {
        return $this->shutdownCount;
    }
}
