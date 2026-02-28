<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

final class TestStatusRegistry
{
    /**
     * @var array<string, array{status: string, message: ?string}>
     */
    private array $statuses = [];

    public function clear(string $testId) : void
    {
        unset($this->statuses[$testId]);
    }

    public function getMessage(string $testId) : ?string
    {
        return $this->statuses[$testId]['message'] ?? null;
    }

    public function getStatus(string $testId) : string
    {
        return $this->statuses[$testId]['status'] ?? 'passed';
    }

    public function setStatus(string $testId, string $status, ?string $message = null) : void
    {
        $this->statuses[$testId] = ['status' => $status, 'message' => $message];
    }
}
