<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Attribute\ProcessAttribute;
use Flow\Telemetry\Resource\ResourceDetector;

/**
 * Detects process information.
 *
 * Detects the following attributes:
 * - process.pid: Process ID
 * - process.executable.path: Path to the PHP binary
 * - process.runtime.name: Always "PHP"
 * - process.runtime.version: PHP version
 * - process.command: The script being executed
 * - process.owner: Process owner username (POSIX systems only)
 *
 * Example output:
 * ```
 * process.pid: 12345
 * process.executable.path: /usr/bin/php
 * process.runtime.name: PHP
 * process.runtime.version: 8.3.0
 * process.command: /var/www/index.php
 * process.owner: www-data
 * ```
 */
final readonly class ProcessDetector implements ResourceDetector
{
    public function detect(): Resource
    {
        $attributes = [];

        $pid = \getmypid();

        if ($pid !== false) {
            $attributes[ProcessAttribute::PID->value] = $pid;
        }

        $attributes[ProcessAttribute::EXECUTABLE_PATH->value] = PHP_BINARY;
        $attributes[ProcessAttribute::EXECUTABLE_NAME->value] = \basename(PHP_BINARY);

        $attributes[ProcessAttribute::RUNTIME_NAME->value] = 'PHP';
        $attributes[ProcessAttribute::RUNTIME_VERSION->value] = PHP_VERSION;

        $command = $this->detectCommand();

        if ($command !== null) {
            $attributes[ProcessAttribute::COMMAND->value] = $command;
        }

        $commandArgs = $this->detectCommandArgs();

        if ($commandArgs !== null) {
            $attributes[ProcessAttribute::COMMAND_ARGS->value] = $commandArgs;
        }

        $owner = $this->detectOwner();

        if ($owner !== null) {
            $attributes[ProcessAttribute::OWNER->value] = $owner;
        }

        return Resource::create($attributes);
    }

    private function detectCommand(): ?string
    {
        if (isset($_SERVER['SCRIPT_FILENAME']) && \is_string($_SERVER['SCRIPT_FILENAME'])) {
            return $_SERVER['SCRIPT_FILENAME'];
        }

        global $argv;

        if (isset($argv[0]) && \is_string($argv[0])) {
            return $argv[0];
        }

        return null;
    }

    /**
     * @return null|array<string>
     */
    private function detectCommandArgs(): ?array
    {
        global $argv;

        if (!isset($argv) || !\is_array($argv) || \count($argv) === 0) {
            return null;
        }

        /** @var array<string> $result */
        $result = \array_values(\array_filter($argv, 'is_string'));

        return \count($result) > 0 ? $result : null;
    }

    private function detectOwner(): ?string
    {
        if (!\function_exists('posix_getuid') || !\function_exists('posix_getpwuid')) {
            return null;
        }

        $uid = \posix_getuid();
        $pwuid = \posix_getpwuid($uid);

        if ($pwuid === false) {
            return null;
        }

        return $pwuid['name'];
    }
}
