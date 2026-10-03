<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\PositiveIntegerType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\NullType;

use function array_key_exists;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function strlen;

/**
 * The PHP kind a physical value of a type has - the kinds flow_php's builders store, so both backends refuse the
 * same physicals.
 */
final readonly class PhysicalKind
{
    /**
     * @param Type<mixed> $type
     */
    public function accepts(Type $type, mixed $physical): bool
    {
        if ($physical === null) {
            return true;
        }

        $base = $type instanceof OptionalType ? $type->base() : $type;

        return match (true) {
            $base instanceof IntegerType,
            $base instanceof PositiveIntegerType,
            $base instanceof DateTimeType,
            $base instanceof DateType,
            $base instanceof TimeType,
                => is_int($physical),
            $base instanceof FloatType => is_float($physical),
            $base instanceof BooleanType => is_bool($physical),
            $base instanceof UuidType => is_string($physical) && strlen($physical) === 16,
            $base instanceof NullType => true,
            $base instanceof ListType => is_array($physical) && $this->all($base->element(), $physical),
            $base instanceof MapType => is_array($physical) && $this->all($base->value(), $physical),
            $base instanceof StructureType => is_array($physical) && $this->elements($base, $physical),
            $base instanceof ArrayType => is_array($physical),
            default => is_string($physical),
        };
    }

    /**
     * @param Type<mixed> $type
     * @param array<array-key, mixed> $physicals
     */
    public function all(Type $type, array $physicals): bool
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            if (!$this->accepts($type, $physical)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param StructureType<mixed> $type
     * @param array<array-key, mixed> $physical
     */
    public function elements(StructureType $type, array $physical): bool
    {
        foreach ($type->elements() as $element) {
            if (
                array_key_exists($element->name, $physical)
                && !$this->accepts($element->type, $physical[$element->name])
            ) {
                return false;
            }
        }

        return true;
    }
}
