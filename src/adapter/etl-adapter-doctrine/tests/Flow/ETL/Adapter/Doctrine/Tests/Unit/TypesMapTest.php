<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

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
use Doctrine\DBAL\Types\JsonType as DbalJsonType;
use Doctrine\DBAL\Types\SmallFloatType;
use Doctrine\DBAL\Types\SmallIntType;
use Doctrine\DBAL\Types\StringType as DoctrineStringType;
use Doctrine\DBAL\Types\TextType;
use Doctrine\DBAL\Types\TimeImmutableType;
use Doctrine\DBAL\Types\TimeType as DoctrineTimeType;
use Flow\ETL\Adapter\Doctrine\TypesMap;
use Flow\ETL\Exception\InvalidArgumentException;
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
use PHPUnit\Framework\TestCase;
use stdClass;

use function array_keys;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\xml_schema;

final class TypesMapTest extends TestCase
{
    public function test_complete_dbal_to_flow_type_conversion_workflow(): void
    {
        $typesMap = new TypesMap([]);

        foreach (TypesMap::DBAL_TYPES as $dbalType => $expectedFlowType) {
            $result = $typesMap->toFlowType($dbalType);
            static::assertInstanceOf(
                $expectedFlowType,
                $result,
                "Failed to convert {$dbalType} to {$expectedFlowType}",
            );
        }
    }

    public function test_complete_flow_to_dbal_type_conversion_workflow(): void
    {
        $typesMap = new TypesMap([]);

        foreach (TypesMap::FLOW_TYPES as $flowType => $expectedDbalType) {
            $result = $typesMap->toDbalType($flowType);
            static::assertSame($expectedDbalType, $result, "Failed to convert {$flowType} to {$expectedDbalType}");
        }
    }

    public function test_constructor_validates_dbal_type_class_names(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('"InvalidClass" is not a valid Doctrine DBAL type.');

        new TypesMap([
            StringType::class => 'InvalidClass',
        ]);
    }

    public function test_constructor_validates_dbal_type_with_non_dbal_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('"stdClass" is not a valid Doctrine DBAL type.');

        new TypesMap([
            StringType::class => stdClass::class,
        ]);
    }

    public function test_constructor_validates_flow_type_class_names(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('"InvalidClass" is not a valid type.');

        new TypesMap([
            'InvalidClass' => DoctrineStringType::class,
        ]);
    }

    public function test_constructor_validates_flow_type_with_non_string_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('"stdClass" is not a valid type.');

        new TypesMap([
            stdClass::class => DoctrineStringType::class,
        ]);
    }

    public function test_constructor_with_custom_map_uses_provided_mappings(): void
    {
        $customMap = [
            StringType::class => TextType::class,
            IntegerType::class => BigIntType::class,
        ];

        $typesMap = new TypesMap($customMap);

        $stringResult = $typesMap->toDbalType(StringType::class);
        $integerResult = $typesMap->toDbalType(IntegerType::class);

        static::assertSame(TextType::class, $stringResult);
        static::assertSame(BigIntType::class, $integerResult);
    }

    public function test_constructor_with_empty_map_uses_default_flow_types(): void
    {
        $typesMap = new TypesMap([]);

        $result = $typesMap->toDbalType(StringType::class);

        static::assertSame(DoctrineStringType::class, $result);
    }

    public function test_constructor_with_mixed_valid_and_invalid_types(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('"InvalidFlowType" is not a valid type.');

        new TypesMap([
            StringType::class => TextType::class,
            'InvalidFlowType' => DoctrineStringType::class,
        ]);
    }

    public function test_custom_mapping_overrides_default_behavior(): void
    {
        $customMap = [
            StringType::class => TextType::class,
            IntegerType::class => BigIntType::class,
        ];

        $typesMap = new TypesMap($customMap);

        static::assertSame(TextType::class, $typesMap->toDbalType(StringType::class));
        static::assertSame(BigIntType::class, $typesMap->toDbalType(IntegerType::class));

        $this->expectException(BaseInvalidArgumentException::class);
        $this->expectExceptionMessage('"' . BooleanType::class . '" is not a valid type.');

        $typesMap->toDbalType(BooleanType::class);
    }

    public function test_default_dbal_types_constant_mapping(): void
    {
        $expectedMappings = [
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
            DbalJsonType::class => JsonType::class,
            BlobType::class => StringType::class,
            DecimalType::class => FloatType::class,
        ];

        static::assertSame($expectedMappings, TypesMap::DBAL_TYPES);
    }

    public function test_default_flow_types_constant_mapping(): void
    {
        $expectedMappings = [
            StringType::class => DoctrineStringType::class,
            IntegerType::class => DoctrineIntegerType::class,
            FloatType::class => DoctrineFloatType::class,
            BooleanType::class => DoctrineBooleanType::class,
            DateType::class => DateImmutableType::class,
            TimeType::class => TimeImmutableType::class,
            DateTimeType::class => DateTimeImmutableType::class,
            UuidType::class => GuidType::class,
            TimeZoneType::class => DoctrineStringType::class,
            JsonType::class => DbalJsonType::class,
            XMLType::class => DoctrineStringType::class,
            XMLElementType::class => DoctrineStringType::class,
            HTMLType::class => DoctrineStringType::class,
            HTMLElementType::class => DoctrineStringType::class,
            EnumType::class => DoctrineStringType::class,
            ListType::class => DbalJsonType::class,
            MapType::class => DbalJsonType::class,
            StructureType::class => DbalJsonType::class,
        ];

        static::assertSame($expectedMappings, TypesMap::FLOW_TYPES);
    }

    public function test_edge_case_with_single_mapping(): void
    {
        $typesMap = new TypesMap([
            StringType::class => TextType::class,
        ]);

        $result = $typesMap->toDbalType(StringType::class);

        static::assertSame(TextType::class, $result);
    }

    public function test_to_dbal_type_throws_exception_for_unknown_flow_type(): void
    {
        $typesMap = new TypesMap([
            StringType::class => DoctrineStringType::class,
        ]);

        $this->expectException(BaseInvalidArgumentException::class);
        $this->expectExceptionMessage('"' . IntegerType::class . '" is not a valid type.');

        $typesMap->toDbalType(IntegerType::class);
    }

    public function test_to_dbal_type_with_custom_mapping(): void
    {
        $typesMap = new TypesMap([
            StringType::class => TextType::class,
        ]);

        $result = $typesMap->toDbalType(StringType::class);

        static::assertSame(TextType::class, $result);
    }

    public function test_to_dbal_type_with_valid_flow_type(): void
    {
        $typesMap = new TypesMap([]);

        $result = $typesMap->toDbalType(StringType::class);

        static::assertSame(DoctrineStringType::class, $result);
    }

    public function test_to_flow_type_throws_exception_for_unknown_dbal_type(): void
    {
        $typesMap = new TypesMap([]);

        $this->expectException(BaseInvalidArgumentException::class);
        $this->expectExceptionMessage('"UnknownType" is not a valid Doctrine DBAL type.');

        $typesMap->toFlowType('UnknownType');
    }

    public function test_to_flow_type_with_extended_dbal_types(): void
    {
        $typesMap = new TypesMap([]);

        $textResult = $typesMap->toFlowType(TextType::class);
        $bigIntResult = $typesMap->toFlowType(BigIntType::class);
        $smallIntResult = $typesMap->toFlowType(SmallIntType::class);
        $smallFloatResult = $typesMap->toFlowType(SmallFloatType::class);
        $dateImmutableResult = $typesMap->toFlowType(DateImmutableType::class);
        $timeImmutableResult = $typesMap->toFlowType(TimeImmutableType::class);
        $dateTimeImmutableResult = $typesMap->toFlowType(DateTimeImmutableType::class);
        $dateTimeTzImmutableResult = $typesMap->toFlowType(DateTimeTzImmutableType::class);
        $guidResult = $typesMap->toFlowType(GuidType::class);
        $blobResult = $typesMap->toFlowType(BlobType::class);
        $decimalResult = $typesMap->toFlowType(DecimalType::class);

        static::assertInstanceOf(StringType::class, $textResult);
        static::assertInstanceOf(IntegerType::class, $bigIntResult);
        static::assertInstanceOf(IntegerType::class, $smallIntResult);
        static::assertInstanceOf(FloatType::class, $smallFloatResult);
        static::assertInstanceOf(DateType::class, $dateImmutableResult);
        static::assertInstanceOf(TimeType::class, $timeImmutableResult);
        static::assertInstanceOf(DateTimeType::class, $dateTimeImmutableResult);
        static::assertInstanceOf(DateTimeType::class, $dateTimeTzImmutableResult);
        static::assertInstanceOf(UuidType::class, $guidResult);
        static::assertInstanceOf(StringType::class, $blobResult);
        static::assertInstanceOf(FloatType::class, $decimalResult);
    }

    public function test_to_flow_type_with_valid_dbal_type(): void
    {
        $typesMap = new TypesMap([]);

        $result = $typesMap->toFlowType(DoctrineStringType::class);

        static::assertInstanceOf(StringType::class, $result);
    }

    public function test_to_flow_type_with_various_dbal_types(): void
    {
        $typesMap = new TypesMap([]);

        $stringResult = $typesMap->toFlowType(DoctrineStringType::class);
        $integerResult = $typesMap->toFlowType(DoctrineIntegerType::class);
        $floatResult = $typesMap->toFlowType(DoctrineFloatType::class);
        $booleanResult = $typesMap->toFlowType(DoctrineBooleanType::class);
        $dateResult = $typesMap->toFlowType(DoctrineDateType::class);
        $timeResult = $typesMap->toFlowType(DoctrineTimeType::class);
        $dateTimeResult = $typesMap->toFlowType(DoctrineDateTimeType::class);
        $jsonResult = $typesMap->toFlowType(DbalJsonType::class);

        static::assertInstanceOf(StringType::class, $stringResult);
        static::assertInstanceOf(IntegerType::class, $integerResult);
        static::assertInstanceOf(FloatType::class, $floatResult);
        static::assertInstanceOf(BooleanType::class, $booleanResult);
        static::assertInstanceOf(DateType::class, $dateResult);
        static::assertInstanceOf(TimeType::class, $timeResult);
        static::assertInstanceOf(DateTimeType::class, $dateTimeResult);
        static::assertInstanceOf(JsonType::class, $jsonResult);
    }

    public function test_type_map_preserves_mapping_order(): void
    {
        $customMap = [
            StringType::class => TextType::class,
            IntegerType::class => BigIntType::class,
        ];

        $typesMap = new TypesMap($customMap);

        static::assertSame(TextType::class, $typesMap->toDbalType(StringType::class));
        static::assertSame(BigIntType::class, $typesMap->toDbalType(IntegerType::class));
    }

    public function test_flow_schema_types_maps_schema_definitions_to_dbal_types(): void
    {
        $types = (new TypesMap([]))->flowSchemaTypes(schema(
            int_schema('id'),
            string_schema('name'),
            xml_schema('body'),
        ));

        static::assertSame(['id', 'name', 'body'], array_keys($types));
        static::assertInstanceOf(DoctrineIntegerType::class, $types['id']);
        static::assertInstanceOf(DoctrineStringType::class, $types['name']);
        static::assertInstanceOf(DoctrineStringType::class, $types['body']);
    }
}
