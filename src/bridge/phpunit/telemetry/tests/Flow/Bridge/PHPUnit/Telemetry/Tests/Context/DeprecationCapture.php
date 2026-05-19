<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Context;

use function restore_error_handler;
use function set_error_handler;

use const E_USER_DEPRECATED;

final class DeprecationCapture
{
    /**
     * @template T
     *
     * @param callable():T $callback
     *
     * @return array{result: T, message: null|string}
     */
    public static function around(callable $callback): array
    {
        $captured = null;

        set_error_handler(static function (int $type, string $message) use (&$captured): bool {
            if ($type === E_USER_DEPRECATED) {
                $captured = $message;

                return true;
            }

            return false;
        }, E_USER_DEPRECATED);

        try {
            $result = $callback();
        } finally {
            restore_error_handler();
        }

        return ['result' => $result, 'message' => $captured];
    }

    public static function silence(callable $callback): void
    {
        set_error_handler(static fn(): bool => true, E_USER_DEPRECATED);

        try {
            $callback();
        } finally {
            restore_error_handler();
        }
    }
}
