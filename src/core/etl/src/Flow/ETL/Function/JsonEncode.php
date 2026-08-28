<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Value\Json;
use JsonException;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function is_array;
use function is_object;
use function json_encode;

final class JsonEncode implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $flags;

    public function __construct(
        private readonly ScalarFunction $value,
        ScalarFunction|int $flags = JSON_THROW_ON_ERROR,
    ) {
        $this->flags = $flags instanceof ScalarFunction ? $flags : lit($flags);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->flags];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $value = type_bare($this->value->returns());

        $isContainer =
            $value instanceof ListType
            || $value instanceof MapType
            || $value instanceof StructureType
            || $value instanceof ArrayType
            || $value instanceof JsonType;

        return type_optional($isContainer ? type_json() : type_string());
    }

    public function eval(Row $row, FlowContext $context): Json|string|null
    {
        $value = (new Parameter($this->value))->eval($row, $context);
        $flags = (int) (new Parameter($this->flags))->asInt($row, $context);

        if ($value === null) {
            return null;
        }

        try {
            $encoded = json_encode($value, $flags);

            if ($encoded === false) {
                throw new InvalidArgumentException('JsonEncode error: json_encode returned false');
            }

            if (is_array($value) || is_object($value)) {
                return new Json($encoded);
            }

            return $encoded;
        } catch (JsonException $e) {
            throw new InvalidArgumentException('JsonEncode error: ' . $e->getMessage());
        }
    }
}
