<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use function array_key_exists;
use function assert;
use function is_array;

final readonly class StructPhysical implements Physical
{
    /**
     * @param array<array-key, Physical> $elements in element order
     */
    public function __construct(
        private array $elements,
    ) {}

    public function toPhysical(mixed $value): mixed
    {
        assert(is_array($value));

        $physicals = [];

        foreach ($this->elements as $name => $element) {
            if (array_key_exists($name, $value)) {
                $physicals[$name] = $value[$name] === null ? null : $element->toPhysical($value[$name]);
            }
        }

        return $physicals;
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_array($physical));

        $values = [];

        foreach ($this->elements as $name => $element) {
            if (array_key_exists($name, $physical)) {
                $values[$name] = $physical[$name] === null ? null : $element->fromPhysical($physical[$name]);
            }
        }

        return $values;
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            if ($physical === null) {
                $values[] = null;

                continue;
            }

            assert(is_array($physical));

            $value = [];

            foreach ($this->elements as $name => $element) {
                if (array_key_exists($name, $physical)) {
                    $value[$name] = $physical[$name] === null ? null : $element->fromPhysical($physical[$name]);
                }
            }

            $values[] = $value;
        }

        return $values;
    }
}
