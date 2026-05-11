<?php

declare(strict_types=1);

namespace Flow\Telemetry;

final class ObjectExtractor
{
    public static function shortName(object $object): string
    {
        if (str_contains($object::class, '@anonymous')) {
            return 'class@anonymous';
        }

        $shortName = strrchr($object::class, '\\');

        return $shortName !== false ? substr($shortName, 1) : $object::class;
    }
}
