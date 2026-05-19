<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTime;
use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Row;
use Flow\Types\Exception\CastingException;
use Flow\Types\Type;
use stdClass;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_xml;
use function gettype;
use function json_encode;
use function mb_strtolower;

final class Cast extends ScalarFunctionChain
{
    /**
     * @param mixed $value
     * @param string|Type<mixed> $type
     */
    public function __construct(
        private readonly mixed $value,
        private readonly Type|string $type,
    ) {}

    /**
     * @throws InvalidArgumentException
     * @throws \JsonException
     */
    public function eval(Row $row, FlowContext $context): ?ScalarResult
    {
        $value = (new Parameter($this->value))->eval($row, $context);

        $type = $this->type;

        if (null === $value) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Cast function requires non-null value'));
        }

        if ($type instanceof Type) {
            return new ScalarResult($type->cast($value), $type);
        }

        /** @var string $type */
        try {
            $result = match (mb_strtolower($type)) {
                'datetime' => new ScalarResult(type_datetime()->cast($value), type_datetime()),
                'date' => new ScalarResult(match (gettype($value)) {
                    'string' => (new DateTimeImmutable($value))->setTime(0, 0, 0, 0),
                    'integer' => DateTimeImmutable::createFromFormat('U', (string) $value),
                    'object' => match ($value::class) {
                        DateTime::class, DateTimeImmutable::class => $value->setTime(0, 0, 0, 0),
                        default => null,
                    },
                    default => null,
                }, type_date()),
                'timezone' => new ScalarResult(type_time_zone()->cast($value), type_time_zone()),
                'int', 'integer' => new ScalarResult(type_integer()->cast($value), type_integer()),
                'float', 'double', 'real' => new ScalarResult(type_float()->cast($value), type_float()),
                'string' => new ScalarResult(type_string()->cast($value), type_string()),
                'bool', 'boolean' => new ScalarResult(type_boolean()->cast($value), type_boolean()),
                'array' => new ScalarResult(type_array()->cast($value), type_array()),
                'object' => new ScalarResult(
                    type_instance_of(stdClass::class)->cast($value),
                    type_instance_of(stdClass::class),
                ),
                'json' => new ScalarResult(type_json()->cast($value), type_json()),
                'json_pretty' => new ScalarResult(
                    json_encode($value, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT),
                    type_json(),
                ),
                'xml' => new ScalarResult(type_xml()->cast($value), type_xml()),
                default => null,
            };

            if ($result === null) {
                return $context
                    ->functions()
                    ->invalidResult(new InvalidArgumentException('Cast function does not support type: ' . $type));
            }

            return $result;
        } catch (CastingException $e) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Cast function failed: ' . $e->getMessage()));
        }
    }
}
