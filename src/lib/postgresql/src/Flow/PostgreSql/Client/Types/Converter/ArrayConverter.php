<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use function Flow\Types\DSL\{type_list, type_string};
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};
use Flow\Types\Type;

/**
 * @implements ValueConverter<array<array-key, mixed>>
 */
final class ArrayConverter implements ValueConverter
{
    public function flowType() : Type
    {
        return type_list(type_string());
    }

    public function supportedTypes() : array
    {
        return [
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
            PostgreSqlType::JSONB_ARRAY,
        ];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if (!\is_array($value)) {
            return '{}';
        }

        return '{' . \implode(',', \array_map(
            fn ($item) => $this->escapeArrayElement($item),
            $value
        )) . '}';
    }

    /**
     * @return array<array-key, mixed>
     */
    public function toPhp(string $value, PostgreSqlType $type) : array
    {
        if ($value === '{}') {
            return [];
        }

        $inner = \substr($value, 1, -1);
        $elements = $this->parseArrayElements($inner);

        return \array_map(
            fn ($item) => $this->convertElement($item, $type),
            $elements,
        );
    }

    private function convertElement(?string $value, PostgreSqlType $type) : mixed
    {
        if ($value === null) {
            return null;
        }

        return match ($type) {
            PostgreSqlType::INT2_ARRAY,
            PostgreSqlType::INT4_ARRAY,
            PostgreSqlType::INT8_ARRAY => (int) $value,
            PostgreSqlType::FLOAT4_ARRAY,
            PostgreSqlType::FLOAT8_ARRAY => (float) $value,
            PostgreSqlType::BOOL_ARRAY => $value === 't',
            default => $value,
        };
    }

    private function escapeArrayElement(mixed $value) : string
    {
        if ($value === null) {
            return 'NULL';
        }

        if (\is_bool($value)) {
            return $value ? 't' : 'f';
        }

        if (\is_scalar($value) || (\is_object($value) && \method_exists($value, '__toString'))) {
            $str = (string) $value;
        } else {
            $str = '';
        }

        if (\preg_match('/[{},"\\\\ ]/', $str) || $str === '') {
            return '"' . \str_replace(['\\', '"'], ['\\\\', '\\"'], $str) . '"';
        }

        return $str;
    }

    /**
     * @return list<null|string>
     */
    private function parseArrayElements(string $inner) : array
    {
        $elements = [];
        $current = '';
        $inQuotes = false;
        $i = 0;
        $length = \strlen($inner);

        while ($i < $length) {
            $char = $inner[$i];

            if ($inQuotes) {
                if ($char === '\\' && $i + 1 < $length) {
                    $current .= $inner[$i + 1];
                    $i += 2;
                } elseif ($char === '"') {
                    $inQuotes = false;
                    $i++;
                } else {
                    $current .= $char;
                    $i++;
                }
            } elseif ($char === '"') {
                $inQuotes = true;
                $i++;
            } elseif ($char === ',') {
                $elements[] = $current === 'NULL' ? null : $current;
                $current = '';
                $i++;
            } else {
                $current .= $char;
                $i++;
            }
        }

        $elements[] = $current === 'NULL' ? null : $current;

        return $elements;
    }
}
