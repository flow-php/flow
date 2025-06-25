<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\{type_array, type_boolean, type_date, type_datetime, type_float, type_instance_of, type_integer, type_json, type_string, type_xml};
use Flow\ETL\Exception\{InvalidArgumentException};
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Row;
use Flow\Types\Exception\CastingException;
use Flow\Types\{Type};

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
    public function eval(Row $row) : ?ScalarResult
    {
        $value = (new Parameter($this->value))->eval($row);

        $type = $this->type;

        if (null === $value) {
            return null;
        }

        if ($type instanceof Type) {
            return new ScalarResult($type->cast($value), $type);
        }

        /** @var string $type */
        try {
            return match (\mb_strtolower($type)) {
                'datetime' => new ScalarResult(type_datetime()->cast($value), type_datetime()),
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
                'int', 'integer' => new ScalarResult(type_integer()->cast($value), type_integer()),
                'float', 'double', 'real' => new ScalarResult(type_float()->cast($value), type_float()),
                'string' => new ScalarResult(type_string()->cast($value), type_string()),
                'bool', 'boolean' => new ScalarResult(type_boolean()->cast($value), type_boolean()),
                'array' => new ScalarResult(type_array()->cast($value), type_array()),
                'object' => new ScalarResult(type_instance_of(\stdClass::class)->cast($value), type_instance_of(\stdClass::class)),
                'json' => new ScalarResult(type_json()->cast($value), type_json()),
                'json_pretty' => new ScalarResult(\json_encode($value, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT), type_json()),
                'xml' => new ScalarResult(type_xml()->cast($value), type_xml()),
                default => null,
            };
        } catch (CastingException) {
            return null;
        }
    }
}
