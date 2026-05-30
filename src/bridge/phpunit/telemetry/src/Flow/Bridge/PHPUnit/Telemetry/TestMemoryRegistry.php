<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

final class TestMemoryRegistry
{
    /**
     * @var array<string, int>
     */
    private array $startBytes = [];

    public function clear(string $testId): void
    {
        unset($this->startBytes[$testId]);
    }

    public function getStart(string $testId): ?int
    {
        return $this->startBytes[$testId] ?? null;
    }

    public function setStart(string $testId, int $bytes): void
    {
        $this->startBytes[$testId] = $bytes;
    }
}
