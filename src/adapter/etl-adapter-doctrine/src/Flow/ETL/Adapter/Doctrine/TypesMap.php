<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Types\BigIntType;
use Doctrine\DBAL\Types\BlobType;
use Doctrine\DBAL\Types\BooleanType as DoctrineBooleanType;
use Doctrine\DBAL\Types\DateImmutableType;
use Doctrine\DBAL\Types\DateTimeImmutableType;
use Doctrine\DBAL\Types\DateTimeType as DoctrineDateTimeType;
use Doctrine\DBAL\Types\DateTimeTzImmutableType;
use Doctrine\DBAL\Types\DateTimeTzType;
use Doctrine\DBAL\Types\DateType as DoctrineDateType;
use Doctrine\DBAL\Types\DecimalType;
use Doctrine\DBAL\Types\FloatType as DoctrineFloatType;
use Doctrine\DBAL\Types\GuidType;
use Doctrine\DBAL\Types\IntegerType as DoctrineIntegerType;
use Doctrine\DBAL\Types\JsonType as DoctrineJsonType;
use Doctrine\DBAL\Types\SmallFloatType;
use Doctrine\DBAL\Types\SmallIntType;
use Doctrine\DBAL\Types\StringType as DoctrineStringType;
use Doctrine\DBAL\Types\TextType;
use Doctrine\DBAL\Types\TimeImmutableType;
use Doctrine\DBAL\Types\TimeType as DoctrineTimeType;
use Doctrine\DBAL\Types\Type as DbalType;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema;
use Flow\Types\Type as FlowType;
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
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use InvalidArgumentException as BaseInvalidArgumentException;

use function array_flip;
use function array_key_exists;
use function count;
use function is_a;
use function sprintf;

final class TypesMap
{
    /**
     * @var array<class-string<DbalType>, class-string<FlowType<mixed>>>
     */
    public const array DBAL_TYPES = [
        DoctrineStringType::class => StringType::class,
        TextType::class => StringType::class,
        DoctrineIntegerType::class => IntegerType::class,
        BigIntType::class => IntegerType::class,
        SmallIntType::class => IntegerType::class,
        DoctrineFloatType::class => FloatType::class,
        SmallFloatType::class => FloatType::class,
        DoctrineBooleanType::class => BooleanType::class,
        DoctrineDateType::class => DateType::class,
        DateImmutableType::class => DateType::class,
        TimeImmutableType::class => TimeType::class,
        DoctrineTimeType::class => TimeType::class,
        DateTimeImmutableType::class => DateTimeType::class,
        DateTimeTzImmutableType::class => DateTimeType::class,
        DateTimeTzType::class => DateTimeType::class,
        DoctrineDateTimeType::class => DateTimeType::class,
        GuidType::class => UuidType::class,
        DoctrineJsonType::class => JsonType::class,
        BlobType::class => StringType::class,
        DecimalType::class => FloatType::class,
    ];

    /**
     * @var array<class-string<FlowType<mixed>>, class-string<DbalType>>
     */
    public const array FLOW_TYPES = [
        StringType::class => DoctrineStringType::class,
        IntegerType::class => DoctrineIntegerType::class,
        FloatType::class => DoctrineFloatType::class,
        BooleanType::class => DoctrineBooleanType::class,
        DateType::class => DateImmutableType::class,
        TimeType::class => TimeImmutableType::class,
        DateTimeType::class => DateTimeImmutableType::class,
        UuidType::class => GuidType::class,
        TimeZoneType::class => DoctrineStringType::class,
        JsonType::class => DoctrineJsonType::class,
        XMLType::class => DoctrineStringType::class,
        XMLElementType::class => DoctrineStringType::class,
        HTMLType::class => DoctrineStringType::class,
        HTMLElementType::class => DoctrineStringType::class,
        EnumType::class => DoctrineStringType::class,
        ListType::class => DoctrineJsonType::class,
        MapType::class => DoctrineJsonType::class,
        StructureType::class => DoctrineJsonType::class,
    ];

    /**
     * @var array<class-string<FlowType<mixed>>, class-string<DbalType>>
     */
    private array $map;

    /**
     * @param array<string, string> $map
     */
    public function __construct(array $map)
    {
        foreach ($map as $flowType => $dbalType) {
            if (!is_a($flowType, FlowType::class, true)) {
                throw new InvalidArgumentException(sprintf('"%s" is not a valid type.', $flowType));
            }

            if (!is_a($dbalType, DbalType::class, true)) {
                throw new InvalidArgumentException(sprintf('"%s" is not a valid Doctrine DBAL type.', $dbalType));
            }
        }

        if (!count($map)) {
            $this->map = self::FLOW_TYPES;
        } else {
            /** @var array<class-string<FlowType<mixed>>, class-string<DbalType>> $map */
            $this->map = $map;
        }
    }

    /**
     * Build DBAL types array from a schema's definitions.
     *
     * @return array<string, DbalType> Column name => DBAL Type instance
     */
    public function flowSchemaTypes(Schema $schema): array
    {
        $types = [];
        $typeClassToName = array_flip(DbalType::getTypesMap());

        foreach ($schema->definitions() as $definition) {
            $dbalTypeClass = $this->toDbalType($definition->type()::class);

            if (!array_key_exists($dbalTypeClass, $typeClassToName)) {
                throw new BaseInvalidArgumentException(sprintf('DBAL type "%s" is not registered.', $dbalTypeClass));
            }

            $types[$definition->entry()->name()] = DbalType::getType($typeClassToName[$dbalTypeClass]);
        }

        return $types;
    }

    /**
     * @param class-string<FlowType<mixed>> $flowType
     *
     * @return class-string<DbalType>
     */
    public function toDbalType(string $flowType): string
    {
        if (!array_key_exists($flowType, $this->map)) {
            throw new BaseInvalidArgumentException(sprintf('"%s" is not a valid type.', $flowType));
        }

        return $this->map[$flowType];
    }

    /**
     * @param string $dbalType
     *
     * @return FlowType<mixed>
     */
    public function toFlowType(string $dbalType): FlowType
    {
        if (!array_key_exists($dbalType, self::DBAL_TYPES)) {
            throw new BaseInvalidArgumentException(sprintf('"%s" is not a valid Doctrine DBAL type.', $dbalType));
        }

        $type = self::DBAL_TYPES[$dbalType];

        // @mago-expect analysis:unsafe-instantiation
        return new $type();
    }
}
