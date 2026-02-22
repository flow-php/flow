<?php

declare(strict_types=1);

namespace Flow\Telemetry;

final class ObjectExtractor
{
    public static function shortName(object $object) : string
    {
        if (str_contains($object::class, '@anonymous')) {
            return 'class@anonymous';
        }

        $shortName = strrchr($object::class, '\\');
        $className = $shortName !== false ? substr($shortName, 1) : $object::class;

        $result = preg_replace('/([A-Z]+)([A-Z][a-z])/', '$1_$2', $className);
        $result = preg_replace('/([a-z])([A-Z])/', '$1_$2', $result);

        return strtolower($result);
    }
}
