<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use UnitEnum;

use function assert;
use function constant;
use function is_string;

final readonly class EnumPhysical implements Physical
{
    /**
     * @param class-string<\UnitEnum> $class
     */
    public function __construct(
        private string $class,
    ) {}

    public function toPhysical(mixed $value): mixed
    {
        assert($value instanceof UnitEnum);

        return $value->name;
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_string($physical));

        return constant($this->class . '::' . $physical);
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            assert($physical === null || is_string($physical));
            $values[] = $physical === null ? null : constant($this->class . '::' . $physical);
        }

        return $values;
    }
}
