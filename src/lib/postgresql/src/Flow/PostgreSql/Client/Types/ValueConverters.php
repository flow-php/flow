<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types;

use Flow\PostgreSql\Client\Types\Converter\BoolArrayConverter;
use Flow\PostgreSql\Client\Types\Converter\BooleanConverter;
use Flow\PostgreSql\Client\Types\Converter\ByteaConverter;
use Flow\PostgreSql\Client\Types\Converter\CidrConverter;
use Flow\PostgreSql\Client\Types\Converter\DateConverter;
use Flow\PostgreSql\Client\Types\Converter\FloatArrayConverter;
use Flow\PostgreSql\Client\Types\Converter\FloatConverter;
use Flow\PostgreSql\Client\Types\Converter\InetConverter;
use Flow\PostgreSql\Client\Types\Converter\IntArrayConverter;
use Flow\PostgreSql\Client\Types\Converter\IntegerConverter;
use Flow\PostgreSql\Client\Types\Converter\IntervalConverter;
use Flow\PostgreSql\Client\Types\Converter\JsonArrayConverter;
use Flow\PostgreSql\Client\Types\Converter\JsonConverter;
use Flow\PostgreSql\Client\Types\Converter\MoneyConverter;
use Flow\PostgreSql\Client\Types\Converter\MultirangeConverter;
use Flow\PostgreSql\Client\Types\Converter\NumericConverter;
use Flow\PostgreSql\Client\Types\Converter\StringConverter;
use Flow\PostgreSql\Client\Types\Converter\TextArrayConverter;
use Flow\PostgreSql\Client\Types\Converter\TimeConverter;
use Flow\PostgreSql\Client\Types\Converter\TimestampConverter;
use Flow\PostgreSql\Client\Types\Converter\TimestampTzConverter;
use Flow\PostgreSql\Client\Types\Converter\UuidArrayConverter;
use Flow\PostgreSql\Client\Types\Converter\UuidConverter;

final class ValueConverters
{
    /** @var array<int, ValueConverter> */
    private array $pgTypeConverters = [];

    /**
     * @param list<ValueConverter> $converters
     */
    public function __construct(
        array $converters = [],
        private readonly ValueConverter $fallbackConverter = new StringConverter(),
    ) {
        foreach ($converters as $converter) {
            $this->register($converter);
        }
    }

    /**
     * Create with default converters for a specific PostgreSQL version.
     */
    public static function create(PostgreSqlVersion $version = PostgreSqlVersion::V17): self
    {
        return new self(self::defaultConverters($version));
    }

    public function forValueType(ValueType $type): ValueConverter
    {
        if (isset($this->pgTypeConverters[$type->value])) {
            return $this->pgTypeConverters[$type->value];
        }

        if ($this->isArrayType($type)) {
            return $this->pgTypeConverters[ValueType::TEXT_ARRAY->value] ?? $this->fallbackConverter;
        }

        return $this->fallbackConverter;
    }

    /**
     * Check if a converter exists for the given PostgreSQL type.
     */
    public function has(ValueType $type): bool
    {
        return isset($this->pgTypeConverters[$type->value]) || $this->isArrayType($type);
    }

    /**
     * Register a value converter for its supported types.
     */
    public function register(ValueConverter $converter): void
    {
        foreach ($converter->supportedTypes() as $type) {
            $this->pgTypeConverters[$type->value] = $converter;
        }
    }

    /**
     * Unregister a converter for the given PostgreSQL type.
     */
    public function unregister(ValueType $type): void
    {
        unset($this->pgTypeConverters[$type->value]);
    }

    private function isArrayType(ValueType $type): bool
    {
        return match ($type) {
            ValueType::BOOL_ARRAY,
            ValueType::INT2_ARRAY,
            ValueType::INT4_ARRAY,
            ValueType::INT8_ARRAY,
            ValueType::TEXT_ARRAY,
            ValueType::FLOAT4_ARRAY,
            ValueType::FLOAT8_ARRAY,
            ValueType::VARCHAR_ARRAY,
            ValueType::UUID_ARRAY,
            ValueType::JSON_ARRAY,
            ValueType::JSONB_ARRAY,
                => true,
            default => false,
        };
    }

    /**
     * @return list<ValueConverter>
     */
    private static function defaultConverters(PostgreSqlVersion $version): array
    {
        $converters = [
            new StringConverter(),
            new IntegerConverter(),
            new FloatConverter(),
            new BooleanConverter(),
            new DateConverter(),
            new TimeConverter(),
            new TimestampConverter(),
            new TimestampTzConverter(),
            new UuidConverter(),
            new JsonConverter(),
            new ByteaConverter(),
            new BoolArrayConverter(),
            new IntArrayConverter(),
            new FloatArrayConverter(),
            new TextArrayConverter(),
            new UuidArrayConverter(),
            new JsonArrayConverter(),
            new IntervalConverter(),
            new NumericConverter(),
            new MoneyConverter(),
            new InetConverter(),
            new CidrConverter(),
        ];

        if ($version->supportsMultirange()) {
            $converters[] = new MultirangeConverter();
        }

        return $converters;
    }
}
