<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\s;

final class Truncate implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $length;
    private readonly ScalarFunction $ellipsis;

    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|int $length,
        ScalarFunction|string $ellipsis = '...',
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->length = $length instanceof ScalarFunction ? $length : lit($length);
        $this->ellipsis = $ellipsis instanceof ScalarFunction ? $ellipsis : lit($ellipsis);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->length, $this->ellipsis];
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
        $value = (new Parameter($this->value))->asString($row, $context);
        $length = (new Parameter($this->length))->asInt($row, $context);
        $ellipsis = (new Parameter($this->ellipsis))->asString($row, $context);

        if ($value === null) {
            throw new InvalidArgumentException('Truncate function requires non-null value');
        }

        if ($length === null) {
            return $value;
        }

        return s($value)->truncate($length, $ellipsis ?? '...')->toString();
    }
}
