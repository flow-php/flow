<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Layout;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ClassStringType;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\NonEmptyStringType;
use Flow\Types\Type\Logical\NumericStringType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\PositiveIntegerType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

use function sprintf;

final readonly class LayoutFor
{
    /**
     * @param Type<mixed> $type
     */
    public function type(Type $type): Layout
    {
        $base = $type instanceof OptionalType ? $type->base() : $type;

        return match (true) {
            $base instanceof IntegerType,
            $base instanceof PositiveIntegerType,
            $base instanceof DateTimeType,
            $base instanceof TimeType,
                => new Int64Layout(),
            $base instanceof FloatType => new Float64Layout(),
            $base instanceof DateType => new Int32Layout(),
            $base instanceof BooleanType => new BooleanLayout(),
            $base instanceof StringType,
            $base instanceof NonEmptyStringType,
            $base instanceof NumericStringType,
            $base instanceof ClassStringType,
            $base instanceof TimeZoneType,
            $base instanceof JsonType,
            $base instanceof EnumType,
            $base instanceof XMLType,
            $base instanceof XMLElementType,
            $base instanceof HTMLType,
            $base instanceof HTMLElementType,
                => new Utf8Layout(),
            $base instanceof UuidType => new FixedBinary16Layout(),
            default => throw new InvalidArgumentException(sprintf('type %s has no scalar layout', $base->toString())),
        };
    }
}
