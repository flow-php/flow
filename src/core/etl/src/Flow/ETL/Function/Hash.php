<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Value\Json;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function gettype;
use function is_scalar;
use function serialize;

final class Hash implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;

    public function __construct(
        mixed $value,
        private readonly Algorithm $algorithm = new NativePHPHash(),
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->algorithm);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_string());
    }

    public function eval(Row $row, FlowContext $context): ?string
    {
        $value = (new Parameter($this->value))->eval($row, $context);

        if ($value instanceof Json) {
            $value = $value->toArray();
        }

        return match ($value) {
            null => null,
            default => match (gettype($value)) {
                'array', 'object' => $this->algorithm->hash(serialize($value)),
                default => $this->algorithm->hash(is_scalar($value) ? (string) $value : ''),
            },
        };
    }
}
