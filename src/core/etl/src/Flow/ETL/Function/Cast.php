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
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\PHP\Type\{Caster, Type};
use Flow\ETL\Row;

final class Cast extends ScalarFunctionChain
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
            return new ScalarResult($caster->to($type)->value($value), $type);
        }

        /** @var string $type */
        try {
            return match (\mb_strtolower($type)) {
                'datetime' => new ScalarResult($caster->to(type_datetime())->value($value), type_datetime()),
                'date' => new ScalarResult(
                    match (\gettype($value)) {
                        'string' => (new \DateTimeImmutable($value))->setTime(0, 0, 0, 0),
                        'integer' => \DateTimeImmutable::createFromFormat('U', (string) $value),
                        'object' => match ($value::class) {
                            \DateTime::class, \DateTimeImmutable::class => $value->setTime(0, 0, 0, 0),
                            default => null,
                        },
                        default => null,
                    },
                    type_date()
                ),
                'int', 'integer' => new ScalarResult($caster->to(type_integer())->value($value), type_integer()),
                'float', 'double', 'real' => new ScalarResult($caster->to(type_float())->value($value), type_float()),
                'string' => new ScalarResult($caster->to(type_string())->value($value), type_string()),
                'bool', 'boolean' => new ScalarResult($caster->to(type_boolean())->value($value), type_boolean()),
                'array' => new ScalarResult($caster->to(type_array())->value($value), type_array()),
                'object' => new ScalarResult($caster->to(type_object(\stdClass::class))->value($value), type_object(\stdClass::class)),
                'json' => new ScalarResult($caster->to(type_json())->value($value), type_json()),
                'json_pretty' => new ScalarResult(\json_encode($value, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT), type_json()),
                'xml' => new ScalarResult($caster->to(type_xml())->value($value), type_xml()),
                default => null,
            };
        } catch (CastingException) {
            return null;
        }
    }
}
