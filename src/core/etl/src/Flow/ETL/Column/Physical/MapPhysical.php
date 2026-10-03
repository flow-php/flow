<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use function array_combine;
use function array_keys;
use function array_values;
use function assert;
use function is_array;

final readonly class MapPhysical implements Physical
{
    public function __construct(
        private Physical $value,
    ) {}

    public function toPhysical(mixed $value): mixed
    {
        assert(is_array($value));

        $physicals = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($value as $key => $element) {
            $physicals[$key] = $element === null ? null : $this->value->toPhysical($element);
        }

        return $physicals;
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_array($physical));

        return array_combine(array_keys($physical), $this->value->fromPhysicalAll(array_values($physical)));
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            assert($physical === null || is_array($physical));
            $values[] = $physical === null
                ? null
                : array_combine(array_keys($physical), $this->value->fromPhysicalAll(array_values($physical)));
        }

        return $values;
    }
}
