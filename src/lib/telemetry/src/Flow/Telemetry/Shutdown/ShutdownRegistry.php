<?php

declare(strict_types=1);

namespace Flow\Telemetry\Shutdown;

use Flow\Telemetry\Telemetry;
use WeakReference;

use function array_reverse;
use function register_shutdown_function;

/**
 * Weak refs: never keeps Telemetry alive; re-queues to drain after other shutdown functions.
 */
final class ShutdownRegistry
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
