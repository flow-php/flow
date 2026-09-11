<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Schema\Inference\ColumnTypes;
use Flow\ETL\Schema\Inference\InferredTypes;
use Flow\ETL\Schema\Inference\TypeFloor;
use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use Flow\Types\Type\Native\String\StringTypeNarrower;

use function Flow\Types\DSL\type_string;

final class ColumnTypesMother
{
    public static function allStringsFloor(): TypeFloor
    {
        return new TypeFloor(new InferredTypes(type_string()));
    }

    public static function floor(): TypeFloor
    {
        return new TypeFloor(InferredTypes::default());
    }

    /**
     * @param list<string> $names
     */
    public static function fromStrings(array $names = []): ColumnTypes
    {
        return new ColumnTypes($names, new StringTypeNarrower(InferredTypes::default()->toArray()));
    }

    /**
     * Values a format already typed - Excel cells, decoded JSON.
     *
     * @param list<string> $names
     */
    public static function fromTypedValues(array $names = []): ColumnTypes
    {
        return new ColumnTypes($names, new InstanceOfTypeNarrower());
    }
}
