<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types;

use Flow\PostgreSql\Client\Types\Converter\{ArrayConverter, BooleanConverter, ByteaConverter, DateConverter, DateTimeConverter, FloatConverter, IntegerConverter, IntervalConverter, JsonConverter, MultirangeConverter, StringConverter, TimeConverter, UuidConverter};
use Flow\Types\Type;

final readonly class ValueConverters
{
    /** @var array<class-string<Type<mixed>>, ValueConverter<mixed>> */
    private array $flowTypeConverters;

    /** @var array<int, ValueConverter<mixed>> */
    private array $pgTypeConverters;

    /**
     * @param list<ValueConverter<mixed>> $converters
     * @param ValueConverter<string> $fallbackConverter
     */
    public function __construct(
        array $converters,
        private ValueConverter $fallbackConverter = new StringConverter(),
    ) {
        $flowTypeConverters = [];
        $pgTypeConverters = [];

        foreach ($converters as $converter) {
            $flowTypeConverters[$converter->flowType()::class] = $converter;

            foreach ($converter->supportedTypes() as $type) {
                $pgTypeConverters[$type->value] = $converter;
            }
        }

        $this->flowTypeConverters = $flowTypeConverters;
        $this->pgTypeConverters = $pgTypeConverters;
    }

    /**
     * Create with default converters for a specific PostgreSQL version.
     */
    public static function create(PostgreSqlVersion $version = PostgreSqlVersion::V17) : self
    {
        return new self(self::defaultConverters($version));
    }

    /**
     * @param Type<mixed> $type
     *
     * @return ValueConverter<mixed>
     */
    public function forFlowType(Type $type) : ValueConverter
    {
        return $this->flowTypeConverters[$type::class] ?? $this->fallbackConverter;
    }

    /**
     * @return ValueConverter<mixed>
     */
    public function forPostgreSqlType(PostgreSqlType $type) : ValueConverter
    {
        if (isset($this->pgTypeConverters[$type->value])) {
            return $this->pgTypeConverters[$type->value];
        }

        if ($this->isArrayType($type)) {
            return $this->pgTypeConverters[PostgreSqlType::TEXT_ARRAY->value]
                ?? $this->fallbackConverter;
        }

        return $this->fallbackConverter;
    }

    /**
     * Check if a converter exists for the given PostgreSQL type.
     */
    public function hasConverterFor(PostgreSqlType $type) : bool
    {
        return isset($this->pgTypeConverters[$type->value]) || $this->isArrayType($type);
    }

    /**
     * Add a value converter, returning a new instance.
     *
     * @param ValueConverter<mixed> $converter
     */
    public function with(ValueConverter $converter) : self
    {
        return new self(
            [...\array_values($this->flowTypeConverters), $converter],
            $this->fallbackConverter,
        );
    }

    private function isArrayType(PostgreSqlType $type) : bool
    {
        return match ($type) {
            PostgreSqlType::BOOL_ARRAY,
            PostgreSqlType::INT2_ARRAY,
            PostgreSqlType::INT4_ARRAY,
            PostgreSqlType::INT8_ARRAY,
            PostgreSqlType::TEXT_ARRAY,
            PostgreSqlType::FLOAT4_ARRAY,
            PostgreSqlType::FLOAT8_ARRAY,
            PostgreSqlType::VARCHAR_ARRAY,
            PostgreSqlType::UUID_ARRAY,
            PostgreSqlType::JSON_ARRAY,
            PostgreSqlType::JSONB_ARRAY => true,
            default => false,
        };
    }

    /**
     * @return list<ValueConverter<mixed>>
     */
    private static function defaultConverters(PostgreSqlVersion $version) : array
    {
        $converters = [
            new StringConverter(),
            new IntegerConverter(),
            new FloatConverter(),
            new BooleanConverter(),
            new DateConverter(),
            new TimeConverter(),
            new DateTimeConverter(),
            new UuidConverter(),
            new JsonConverter(),
            new ByteaConverter(),
            new ArrayConverter(),
            new IntervalConverter(),
        ];

        if ($version->supportsMultirange()) {
            $converters[] = new MultirangeConverter();
        }

        return $converters;
    }
}
