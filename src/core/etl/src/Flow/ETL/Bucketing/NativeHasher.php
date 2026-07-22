<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use DateTimeInterface;
use Flow\ETL\Hash\NativePHPHash;
use Stringable;

use function array_map;
use function is_bool;
use function is_numeric;
use function is_object;
use function is_string;
use function serialize;

final readonly class NativeHasher implements Hasher
{
    public static function normalize(mixed $value): string
    {
        if ($value === null) {
            return 'null';
        }

        if (is_bool($value)) {
            return 'b:' . ($value ? '1' : '0');
        }

        if (is_numeric($value)) {
            $float = (float) $value;

            // -0.0 == 0.0 but casts to the string "-0"
            return 'n:' . ($float === 0.0 ? '0' : (string) $float);
        }

        if (is_string($value)) {
            return 's:' . $value;
        }

        if ($value instanceof DateTimeInterface) {
            return 'd:' . $value->format('U.u');
        }

        if (is_object($value)) {
            return $value instanceof Stringable
                ? 'o:' . $value::class . ':' . $value->__toString()
                : 'o:' . serialize($value);
        }

        return 'a:' . serialize($value);
    }

    public function hash(array $values): array
    {
        $hashes = [];

        foreach ($values as $rowValues) {
            $hashes[] = NativePHPHash::xxh128(serialize(array_map(self::normalize(...), $rowValues)));
        }

        return $hashes;
    }
}
