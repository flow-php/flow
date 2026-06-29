<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Context;

use function getenv;
use function putenv;

final class RuntimeEnvironment
{
    /**
     * @param array<string, string|null> $server
     * @param array<string, string|null> $env
     */
    public static function with(array $server, array $env, callable $test): void
    {
        $serverBackup = [];

        foreach ($server as $key => $value) {
            $serverBackup[$key] = $_SERVER[$key] ?? null;

            if ($value === null) {
                unset($_SERVER[$key]);
            } else {
                $_SERVER[$key] = $value;
            }
        }

        $envBackup = [];

        foreach ($env as $key => $value) {
            $current = getenv($key);
            $envBackup[$key] = $current === false ? null : $current;

            if ($value === null) {
                putenv($key);
            } else {
                putenv($key . '=' . $value);
            }
        }

        try {
            $test();
        } finally {
            foreach ($serverBackup as $key => $value) {
                if ($value === null) {
                    unset($_SERVER[$key]);
                } else {
                    $_SERVER[$key] = $value;
                }
            }

            foreach ($envBackup as $key => $value) {
                if ($value === null) {
                    putenv($key);
                } else {
                    putenv($key . '=' . $value);
                }
            }
        }
    }
}
