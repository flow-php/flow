<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function count;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\s;

final class StringContainsAny implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $needles;

    /**
     * @param array<string>|ScalarFunction $needles
     */
    public function __construct(ScalarFunction|string $value, ScalarFunction|array $needles)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->needles = $needles instanceof ScalarFunction ? $needles : lit($needles);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->needles];
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
        return (new Nullability())->any(type_boolean(), $this->value->returns(), $this->needles->returns());
    }

    public function eval(Row $row, FlowContext $context): ?bool
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $needles = (new Parameter($this->needles))->asArray($row, $context);

        if ($value === null || $needles === null) {
            return null;
        }

        if (count($needles) === 0) {
            throw new InvalidArgumentException('StringContainsAny function requires a non-empty needles array');
        }

        $typedNeedles = type_list(type_string())->assert($needles);

        return s($value)->containsAny($typedNeedles);
    }
}
