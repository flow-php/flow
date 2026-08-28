<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;
use function mb_strlen;
use function mb_substr;
use function str_repeat;

final class Sanitize implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $placeholder;
    private readonly ScalarFunction $skipCharacters;

    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|string $placeholder,
        ScalarFunction|int|null $skipCharacters = null,
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->placeholder = $placeholder instanceof ScalarFunction ? $placeholder : lit($placeholder);
        $this->skipCharacters = $skipCharacters instanceof ScalarFunction ? $skipCharacters : lit($skipCharacters);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->placeholder, $this->skipCharacters];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_string();
    }

    public function eval(Row $row, FlowContext $context): ?string
    {
        $val = (new Parameter($this->value))->asString($row, $context);
        $placeholder = (new Parameter($this->placeholder))->asString($row, $context);
        $skipCharacters = (new Parameter($this->skipCharacters))->asInt($row, $context);

        if ($val === null) {
            throw new InvalidArgumentException('Sanitize function requires non-null value');
        }

        if ($placeholder === null) {
            throw new InvalidArgumentException('Sanitize function requires non-null placeholder');
        }

        $size = mb_strlen($val);

        if ($skipCharacters !== null && $size > $skipCharacters) {
            return mb_substr($val, 0, $skipCharacters) . str_repeat($placeholder, max(0, $size - $skipCharacters));
        }

        return str_repeat($placeholder, $size);
    }
}
