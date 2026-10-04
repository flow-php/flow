<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use Flow\Types\Value\Uuid;

use function assert;
use function hex2bin;
use function is_string;
use function str_replace;

final readonly class UuidPhysical implements Physical
{
    public function toPhysical(mixed $value): mixed
    {
        assert($value instanceof Uuid);

        return hex2bin(str_replace('-', '', $value->toString()));
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_string($physical));

        return Uuid::fromBytes($physical);
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            assert($physical === null || is_string($physical));
            $values[] = $physical === null ? null : Uuid::fromBytes($physical);
        }

        return $values;
    }
}
