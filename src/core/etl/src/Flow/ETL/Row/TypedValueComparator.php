<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ArrayComparison\ArrayComparison;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\UnionType;

use function Flow\ETL\DSL\date_interval_to_microseconds;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

final readonly class TypedValueComparator
{
    /**
     * @param Type<mixed> $type
     */
    public function equals(Type $type, mixed $left, mixed $right): bool
    {
        if ($left === null || $right === null) {
            return $left === $right;
        }

        if ($type instanceof UnionType) {
            $member = $type->memberFor($left);

            return $member !== null && $member->isValid($right) && $this->equals($member, $left, $right);
        }

        return match (true) {
            $type instanceof ListType,
            $type instanceof MapType,
            $type instanceof StructureType,
                => (new ArrayComparison())->equals(type_array()->assert($left), type_array()->assert($right)),
            $type instanceof JsonType => type_json()->assert($left)->isEqual(type_json()->assert($right)),
            $type instanceof UuidType => type_uuid()->assert($left)->isEqual(type_uuid()->assert($right)),
            $type instanceof DateType => type_date()->assert($left) == type_date()->assert($right),
            $type instanceof DateTimeType => type_datetime()->assert($left) == type_datetime()->assert($right),
            $type instanceof TimeType => date_interval_to_microseconds(type_time()->assert(
                $left,
            )) === date_interval_to_microseconds(type_time()->assert($right)),
            $type instanceof TimeZoneType => type_time_zone()->assert($left)->getName() === type_time_zone()
                ->assert($right)
                ->getName(),
            $type instanceof XMLType => type_xml()->assert($left)->C14N() === type_xml()->assert($right)->C14N(),
            $type instanceof XMLElementType => type_xml_element()->assert($left)->C14N() === type_xml_element()
                ->assert($right)
                ->C14N(),
            // StringType::cast()'s HTMLDocument arm is saveHtml(), which is PHP 8.4 only
            $type instanceof HTMLType => type_string()->cast($left) === type_string()->cast($right),
            $type instanceof HTMLElementType => type_html_element()->assert($left)->C14N() === type_html_element()
                ->assert($right)
                ->C14N(),
            default => $left === $right,
        };
    }
}
