<?php declare(strict_types=1);

namespace Flow\Parquet\Data;

final class ObjectToString
{
    public static function toString(object $object) : string
    {
        if ($object instanceof \Stringable) {
            return (string) $object;
        }

        if ($object instanceof \DateTimeInterface) {
            return $object->format(\DateTimeInterface::RFC3339);
        }

        if ($object instanceof \DateInterval) {
            return $object->format('%R%yY%mM%dDT%hH%iM%sS.%f');
        }

        return \serialize($object);
    }
}
