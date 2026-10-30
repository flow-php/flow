<?php

declare(strict_types=1);

namespace Flow\Telemetry;

final class ObjectExtractor
{
    public static function shortName(object|string $objectOrClass): string
    {
        $class = is_string($objectOrClass) ? $objectOrClass : $objectOrClass::class;

        if (str_contains($class, '@anonymous')) {
            return 'class@anonymous';
        }

        $shortName = strrchr($class, '\\');

        return $shortName !== false ? substr($shortName, 1) : $class;
    }
}
