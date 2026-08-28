<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Value\Json;
use JsonException;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_array;
use function is_array;
use function is_string;
use function json_decode;

final class JsonDecode implements ScalarFunction
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
        return type_array();
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $value = (new Parameter($this->value))->eval($row, $context);
        $flags = (int) (new Parameter($this->flags))->asInt($row, $context);

        if ($value === null) {
            throw new InvalidArgumentException('JsonDecode function requires non-null value');
        }

        if ($value instanceof Json) {
            return $value->toArray();
        }

        if (is_array($value)) {
            return $value;
        }

        if (!is_string($value)) {
            throw new InvalidArgumentException('JsonDecode function requires string, array, or Json value');
        }

        try {
            return json_decode($value, true, 512, $flags);
        } catch (JsonException $e) {
            throw new InvalidArgumentException('JsonDecode error: ' . $e->getMessage());
        }
    }
}
