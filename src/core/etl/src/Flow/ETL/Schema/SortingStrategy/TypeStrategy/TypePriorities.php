<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\SortingStrategy\TypeStrategy;

use Flow\ETL\Schema\Definition;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

final readonly class TypePriorities
{
    /**
     * @var array<class-string<Type<mixed>>, int>
     */
    public const array PRIORITIES = [
        UuidType::class => 1,
        IntegerType::class => 2,
        BooleanType::class => 3,
        FloatType::class => 4,
        DateTimeType::class => 5,
        StringType::class => 6,
        EnumType::class => 7,
        ListType::class => 8,
        JsonType::class => 9,
        MapType::class => 10,
        StructureType::class => 11,
        XMLType::class => 12,
        XMLElementType::class => 13,
        TimeZoneType::class => 14,
    ];

    public const int UNKNOWN_TYPE_PRIORITY = PHP_INT_MAX;

    /**
     * @param array<class-string<Type<mixed>>, int> $priorities
     */
    public function __construct(
        private array $priorities = self::PRIORITIES,
    ) {}

    /**
     * @param Definition<mixed> $definition
     */
    public function for(Definition $definition): int
    {
        return $this->priorities[$definition->type()::class] ?? self::UNKNOWN_TYPE_PRIORITY;
    }
}
