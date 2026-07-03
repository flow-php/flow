<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use function array_pop;
use function count;

/**
 * Aggregates finished-test statuses per open test suite so the suite span can carry the official
 * `test.suite.run.status` attribute. Suites nest, so every finished test counts towards all
 * currently open suites.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/registry/attributes/test/
 */
final class SuiteOutcomeStack
{
    /**
     * @var list<array{total: int, failed: int, skipped: int}>
     */
    private array $frames = [];

    /**
     * @return null|string one of the test.suite.run.status well-known values: success|failure|skipped
     */
    public function pop(): ?string
    {
        $frame = array_pop($this->frames);

        if ($frame === null) {
            return null;
        }

        return match (true) {
            $frame['failed'] > 0 => 'failure',
            $frame['total'] > 0 && $frame['skipped'] === $frame['total'] => 'skipped',
            default => 'success',
        };
    }

    public function push(): void
    {
        $this->frames[] = ['total' => 0, 'failed' => 0, 'skipped' => 0];
    }

    public function recordStatus(string $status): void
    {
        foreach ($this->frames as $index => $frame) {
            $this->frames[$index]['total']++;

            if ($status === 'failed' || $status === 'errored') {
                $this->frames[$index]['failed']++;
            }

            if ($status === 'skipped') {
                $this->frames[$index]['skipped']++;
            }
        }
    }

    public function size(): int
    {
        return count($this->frames);
    }
}
