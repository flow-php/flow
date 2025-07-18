<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Types\{BigIntType, BlobType, BooleanType, DateImmutableType, DateTimeImmutableType, DecimalType, FloatType, GuidType, IntegerType, JsonType, SmallIntType, StringType, TextType, TimeImmutableType, Type, Types};
use Flow\ETL\Schema;
use Flow\Types\Type as FlowType;
use Flow\Types\Type\Logical\{ListType, MapType, StructureType};

final readonly class DbalTypesDetector
{
    public function __construct(private TypesMap $typesMap = new TypesMap([]))
    {
    }

    /**
     * Converts Flow Schema to DBAL Types array.
     * Detects types for columns not already provided in $typesOverride.
     *
     * @param Schema $schema
     * @param array<string, Type> $typesOverride column types that take priority
     *
     * @return array<string, Type> Column name => DBAL Type instance
     */
    public function convert(Schema $schema, array $typesOverride = []) : array
    {
        $detectedTypes = [];

        foreach ($schema->definitions() as $definition) {
            $flowType = $definition->type();
            $columnName = $definition->entry()->name();

            if (array_key_exists($columnName, $typesOverride)) {
                continue;
            }

            if ($this->isNestedType($flowType)) {
                $detectedTypes[$columnName] = Type::getType(Types::JSON);

                continue;
            }

            $dbalTypeClass = $this->typesMap->toDbalType($flowType::class);
            $detectedTypes[$columnName] = Type::getType($this->getTypeConstant($dbalTypeClass));
        }

        return array_merge($detectedTypes, $typesOverride);
    }

    /**
     * Maps DBAL type class to DBAL Types constant.
     *
     * @param class-string<Type> $dbalTypeClass
     */
    private function getTypeConstant(string $dbalTypeClass) : string
    {
        return match ($dbalTypeClass) {
            StringType::class => Types::STRING,
            IntegerType::class => Types::INTEGER,
            FloatType::class => Types::FLOAT,
            BooleanType::class => Types::BOOLEAN,
            DateTimeImmutableType::class => Types::DATETIME_IMMUTABLE,
            DateImmutableType::class => Types::DATE_IMMUTABLE,
            TimeImmutableType::class => Types::TIME_IMMUTABLE,
            GuidType::class => Types::GUID,
            JsonType::class => Types::JSON,
            TextType::class => Types::TEXT,
            BigIntType::class => Types::BIGINT,
            SmallIntType::class => Types::SMALLINT,
            DecimalType::class => Types::DECIMAL,
            BlobType::class => Types::BLOB,
            default => throw new \InvalidArgumentException("Unsupported DBAL type class: {$dbalTypeClass}"),
        };
    }

    /**
     * Checks if a Flow type is a nested type (List, Map, Structure).
     *
     * @param FlowType<mixed> $flowType
     */
    private function isNestedType(FlowType $flowType) : bool
    {
        return $flowType instanceof ListType
            || $flowType instanceof MapType
            || $flowType instanceof StructureType;
    }
}
