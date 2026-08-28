<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\s;

final class Wordwrap implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $width;
    private readonly ScalarFunction $break;
    private readonly ScalarFunction $cut;

    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|int $width,
        ScalarFunction|string $break = "\n",
        ScalarFunction|bool $cut = false,
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->width = $width instanceof ScalarFunction ? $width : lit($width);
        $this->break = $break instanceof ScalarFunction ? $break : lit($break);
        $this->cut = $cut instanceof ScalarFunction ? $cut : lit($cut);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->width, $this->break, $this->cut];
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

    public function eval(Row $row, FlowContext $context): ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $width = type_integer()->assert((new Parameter($this->width))->as($row, $context, type_integer()));
        $break = (new Parameter($this->break))->asString($row, $context);
        $cut = (new Parameter($this->cut))->asBoolean($row, $context);

        if ($value === null) {
            throw new InvalidArgumentException('Wordwrap function requires non-null value');
        }

        if ($width <= 0) {
            return $value;
        }

        if ($break === null) {
            $break = "\n";
        }

        return s($value)->wordwrap($width, $break, $cut)->toString();
    }
}
