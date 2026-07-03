<?php

declare(strict_types=1);

namespace Flow\Telemetry\Shutdown;

use Flow\Telemetry\Telemetry;
use WeakReference;

use function array_reverse;
use function register_shutdown_function;

/**
 * Registers Telemetry instances for shutdown at process end.
 *
 * Instances are held through weak references, so registration does not keep
 * a Telemetry object graph alive - an instance that was garbage collected
 * before process end (a discarded test kernel, a rebooted container) is
 * silently skipped. On invocation the handler re-registers its work at the
 * end of the shutdown-function queue, so telemetry drains after other
 * registered shutdown functions (loggers, sessions, ...) have run.
 */
final class ShutdownHandler
{
    /**
     * @var null|array<int, WeakReference<Telemetry>>
     */
    private static ?array $references = null;

    private function __construct() {}

    public static function register(Telemetry $telemetry): void
    {
        if (self::$references === null) {
            self::$references = [];

            register_shutdown_function(static function (): void {
                register_shutdown_function(self::invoke(...));
            });
        }

        self::$references[] = WeakReference::create($telemetry);
    }

    /**
     * Shut down all still-referenced Telemetry instances, most recent first.
     *
     * Public only so process-end behaviour can be exercised in tests without
     * terminating the PHP process.
     */
    public static function invoke(): void
    {
        $references = self::$references ?? [];
        self::$references = null;

        foreach (array_reverse($references) as $reference) {
            $reference->get()?->shutdown();
        }
    }
}
