<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;
use function str_pad;

final class StrPad implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $length;
    private readonly ScalarFunction $padString;
    private readonly ScalarFunction $type;

    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|int $length,
        ScalarFunction|string $padString = ' ',
        ScalarFunction|int $type = STR_PAD_RIGHT,
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->length = $length instanceof ScalarFunction ? $length : lit($length);
        $this->padString = $padString instanceof ScalarFunction ? $padString : lit($padString);
        $this->type = $type instanceof ScalarFunction ? $type : lit($type);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->length, $this->padString, $this->type];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2], $children[3]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_string();
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $length = (new Parameter($this->length))->asInt($row, $context);
        $padString = (new Parameter($this->padString))->asString($row, $context);
        $type = (new Parameter($this->type))->asInt($row, $context);

        if ($value === null) {
            throw new InvalidArgumentException('StrPad function requires non-null value');
        }

        if ($length === null || $padString === null || $type === null) {
            throw new InvalidArgumentException('StrPad function requires non-null length, padString and type');
        }

        return str_pad($value, $length, $padString, $type);
    }
}
