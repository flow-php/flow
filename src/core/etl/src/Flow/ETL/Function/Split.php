<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Symfony\Component\String\AbstractString;

use function array_map;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\s;

final class Split implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $separator;
    private readonly ScalarFunction $limit;

    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|string $separator,
        ScalarFunction|int $limit = PHP_INT_MAX,
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->separator = $separator instanceof ScalarFunction ? $separator : lit($separator);
        $this->limit = $limit instanceof ScalarFunction ? $limit : lit($limit);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->separator, $this->limit];
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
        return type_list(type_string());
    }

    /**
     * @return null|array<int, string>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $separator = (new Parameter($this->separator))->asString($row, $context);
        $limit = (new Parameter($this->limit))->asInt($row, $context);

        if ($value === null) {
            throw new InvalidArgumentException('Split function requires non-null value');
        }

        if ($separator === null || $limit === null || $separator === '') {
            throw new InvalidArgumentException(
                'Split function requires non-null separator and limit, separator cannot be empty',
            );
        }

        // @mago-ignore analysis:less-specific-return-statement
        return array_map(static fn(AbstractString $s): string => $s->toString(), s($value)->split($separator, $limit));
    }
}
