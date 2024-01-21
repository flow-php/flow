<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\type_array;
use function Flow\ETL\DSL\type_boolean;
use function Flow\ETL\DSL\type_datetime;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_integer;
use function Flow\ETL\DSL\type_json;
use function Flow\ETL\DSL\type_object;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\type_xml;
use Flow\ETL\Exception\CastingException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row;

final class Cast extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly string|Type $type
    ) {
    }

    /**
     * @throws InvalidArgumentException
     * @throws \JsonException
     */
    public function eval(Row $row) : mixed
    {
        $value = $this->ref->eval($row);

        if (null === $value) {
            return null;
        }

        $caster = Caster::default();

        $type = $this->type;

        if ($type instanceof Type) {
            return $caster->to($type)->value($value);
        }

        try {
            return match (\mb_strtolower($type)) {
                'datetime' => $caster->to(type_datetime())->value($value),
                'date' => match (\gettype($value)) {
                    'string' => (new \DateTimeImmutable($value))->setTime(0, 0, 0, 0),
                    'integer' => \DateTimeImmutable::createFromFormat('U', (string) $value),
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
                default => null
            };
        } catch (CastingException $e) {
            return null;
        }
    }
}
