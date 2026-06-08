<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use function array_map;
use function glob;
use function is_dir;
use function mkdir;
use function rmdir;
use function sys_get_temp_dir;
use function uniqid;

/**
 * A unique, self-cleaning temporary directory for tests that exercise the
 * filter's generated-code cache. Keeps temp-file bookkeeping out of the test
 * cases so they stay free of private members.
 */
final class TempDir
{
    private function __construct(
        private readonly string $path,
    ) {}

    public static function create(): self
    {
        $path = sys_get_temp_dir() . '/flow_telemetry_filter_test_' . uniqid();
        mkdir($path);

        return new self($path);
    }

    public function path(): string
    {
        return $this->path;
    }

    /**
     * @return array<string>
     */
    public function files(): array
    {
        return glob($this->path . '/*') ?: [];
    }

    public function remove(): void
    {
        array_map('unlink', $this->files());

        if (is_dir($this->path)) {
            rmdir($this->path);
        }
    }
}
