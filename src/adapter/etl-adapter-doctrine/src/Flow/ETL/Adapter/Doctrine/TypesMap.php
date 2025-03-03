<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Types\{Type as DbalType};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\{DateTimeType, DateType, JsonType, ListType, MapType, StructureType, TimeType, UuidType, XMLElementType, XMLType};
use Flow\ETL\PHP\Type\Native\{BooleanType, FloatType, IntegerType, StringType};
use Flow\ETL\PHP\Type\Type as FlowType;

final class TypesMap
{
    /**
     * @var array<class-string<DbalType>, class-string<FlowType<mixed>>>
     */
    public const DBAL_TYPES = [
        \Doctrine\DBAL\Types\StringType::class => StringType::class,
        \Doctrine\DBAL\Types\TextType::class => StringType::class,
        \Doctrine\DBAL\Types\IntegerType::class => IntegerType::class,
        \Doctrine\DBAL\Types\BigIntType::class => IntegerType::class,
        \Doctrine\DBAL\Types\SmallIntType::class => IntegerType::class,
        \Doctrine\DBAL\Types\FloatType::class => FloatType::class,
        \Doctrine\DBAL\Types\SmallFloatType::class => FloatType::class,
        \Doctrine\DBAL\Types\BooleanType::class => BooleanType::class,
        \Doctrine\DBAL\Types\DateType::class => DateType::class,
        \Doctrine\DBAL\Types\DateImmutableType::class => DateType::class,
        \Doctrine\DBAL\Types\TimeImmutableType::class => TimeType::class,
        \Doctrine\DBAL\Types\TimeType::class => TimeType::class,
        \Doctrine\DBAL\Types\DateTimeImmutableType::class => DateTimeType::class,
        \Doctrine\DBAL\Types\DateTimeTzImmutableType::class => DateTimeType::class,
        \Doctrine\DBAL\Types\DateTimeTzType::class => DateTimeType::class,
        \Doctrine\DBAL\Types\DateTimeType::class => DateTimeType::class,
        \Doctrine\DBAL\Types\GuidType::class => UuidType::class,
        \Doctrine\DBAL\Types\JsonType::class => JsonType::class,
        \Doctrine\DBAL\Types\BlobType::class => StringType::class,
        \Doctrine\DBAL\Types\DecimalType::class => FloatType::class,
    ];

    /**
     * @var array<class-string<FlowType<mixed>>, class-string<DbalType>>
     */
    public const FLOW_TYPES = [
        StringType::class => \Doctrine\DBAL\Types\StringType::class,
        IntegerType::class => \Doctrine\DBAL\Types\IntegerType::class,
        FloatType::class => \Doctrine\DBAL\Types\FloatType::class,
        BooleanType::class => \Doctrine\DBAL\Types\BooleanType::class,
        DateType::class => \Doctrine\DBAL\Types\DateImmutableType::class,
        TimeType::class => \Doctrine\DBAL\Types\TimeImmutableType::class,
        DateTimeType::class => \Doctrine\DBAL\Types\DateTimeImmutableType::class,
        UuidType::class => \Doctrine\DBAL\Types\GuidType::class,
        JsonType::class => \Doctrine\DBAL\Types\JsonType::class,
        XMLType::class => \Doctrine\DBAL\Types\StringType::class,
        XMLElementType::class => \Doctrine\DBAL\Types\StringType::class,
        ListType::class => \Doctrine\DBAL\Types\JsonType::class,
        MapType::class => \Doctrine\DBAL\Types\JsonType::class,
        StructureType::class => \Doctrine\DBAL\Types\JsonType::class,
    ];

    /**
     * @var array<class-string<FlowType<mixed>>, class-string<DbalType>>
     */
    private array $map;

    /**
     * @param array<class-string<FlowType<mixed>>, class-string<DbalType>> $map
     */
    public function __construct(array $map)
    {
        foreach ($map as $flowType => $dbalType) {
            if (!\is_a($flowType, FlowType::class, true)) {
                throw new InvalidArgumentException(\sprintf('"%s" is not a valid type.', $flowType));
            }

            if (!\is_a($dbalType, DbalType::class, true)) {
                throw new InvalidArgumentException(\sprintf('"%s" is not a valid Doctrine DBAL type.', $dbalType::class));
            }
        }

        if (!\count($map)) {
            $this->map = self::FLOW_TYPES;
        } else {
            $this->map = $map;
        }
    }

    /**
     * @param class-string<FlowType<mixed>> $flowType
     *
     * @return class-string<DbalType>
     */
    public function toDbalType(string $flowType) : string
    {
        if (!\array_key_exists($flowType, $this->map)) {
            throw new \InvalidArgumentException(\sprintf('"%s" is not a valid type.', $flowType));
        }

        return $this->map[$flowType];
    }

    /**
     * @param class-string<DbalType> $dbalType
     *
     * @return FlowType<mixed>
     */
    public function toFlowType(string $dbalType) : FlowType
    {
        if (!\array_key_exists($dbalType, self::DBAL_TYPES)) {
            throw new \InvalidArgumentException(\sprintf('"%s" is not a valid Doctrine DBAL type.', $dbalType));
        }

        $type = self::DBAL_TYPES[$dbalType];

        return new $type;
    }
}
