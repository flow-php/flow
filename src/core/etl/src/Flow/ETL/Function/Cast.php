<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_array,
    type_boolean,
    type_date,
    type_datetime,
    type_float,
    type_integer,
    type_json,
    type_object,
    type_string,
    type_xml};
use Flow\ETL\Exception\{CastingException, InvalidArgumentException};
use Flow\ETL\Function\ScalarFunction\TypedScalarFunction;
use Flow\ETL\PHP\Type\{Caster, Type};
use Flow\ETL\Row;

final class Cast extends ScalarFunctionChain implements TypedScalarFunction
{
    /**
     * @param mixed $value
     * @param string|Type<mixed> $type
     */
    public function __construct(
        private readonly mixed $value,
        private readonly Type|string $type,
    ) {
    }

    /**
     * @throws InvalidArgumentException
     * @throws \JsonException
     */
    public function eval(Row $row) : mixed
    {
        $value = (new Parameter($this->value))->eval($row);
        $type = $this->type;

        if (null === $value) {
            return null;
        }

        $caster = Caster::default();

        if ($type instanceof Type) {
            return $caster->to($type)->value($value);
        }

        /** @var string $type */
        try {
            return match (\mb_strtolower($type)) {
                'datetime' => $caster->to(type_datetime())->value($value),
                'date' => match (\gettype($value)) {
                    'string' => (new \DateTimeImmutable($value))->setTime(0, 0, 0, 0),
                    'integer' => \DateTimeImmutable::createFromFormat('U', (string) $value),
                    'object' => match ($value::class) {
                        \DateTime::class, \DateTimeImmutable::class => $value->setTime(0, 0, 0, 0),
                        default => null,
                    },
                    default => null,
                },
                'int', 'integer' => $caster->to(type_integer())->value($value),
                'float', 'double', 'real' => $caster->to(type_float())->value($value),
                'string' => $caster->to(type_string())->value($value),
                'bool', 'boolean' => $caster->to(type_boolean())->value($value),
                'array' => $caster->to(type_array())->value($value),
                'object' => $caster->to(type_object(\stdClass::class))->value($value),
                'json' => $caster->to(type_json())->value($value),
                'json_pretty' => \json_encode($value, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT),
                'xml' => $caster->to(type_xml())->value($value),
                default => null,
            };
        } catch (CastingException) {
            return null;
        }
    }

    /**
     * @returns Type<mixed>
     */
    public function returns() : Type
    {
        if ($this->type instanceof Type) {
            return $this->type;
        }

        return match (\mb_strtolower($this->type)) {
            'datetime' => type_datetime(),
            'date' => type_date(),
            'int', 'integer' => type_integer(),
            'float', 'double', 'real' => type_float(),
            'string' => type_string(),
            'bool', 'boolean' => type_boolean(),
            'array' => type_array(),
            'object' => type_object(\stdClass::class),
            'json' => type_json(),
            'json_pretty' => type_json(),
            'xml' => type_xml(),
            default => type_string(),
        };
    }
}
