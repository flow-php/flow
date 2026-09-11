<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Context;

use RuntimeException;

use function array_diff;
use function array_values;
use function count;
use function glob;
use function unlink;

/**
 * Scenarios write to a uniqid() path and return void, so a test identifies their output by what
 * appeared under the pattern while the scenario ran.
 */
final class WrittenFile
{
    /** @var list<string> */
    private array $before;

    public function __construct(
        private readonly string $pattern,
    ) {
        $this->before = glob($this->pattern) ?: [];
    }

    public function path(): string
    {
        $created = array_values(array_diff(glob($this->pattern) ?: [], $this->before));

        if (count($created) !== 1) {
            throw new RuntimeException(
                'Expected exactly one file created under ' . $this->pattern . ', got ' . count($created),
            );
        }

        return $created[0];
    }

    public function remove(): void
    {
        foreach (array_diff(glob($this->pattern) ?: [], $this->before) as $file) {
            unlink($file);
        }
    }
}
