<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\String\StringStyles;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_string;

final class StringStyle implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $string;
    private readonly ScalarFunction $style;

    public function __construct(ScalarFunction|string $string, ScalarFunction|string|StringStyles $style)
    {
        $this->string = $string instanceof ScalarFunction ? $string : lit($string);
        $this->style = $style instanceof ScalarFunction ? $style : lit($style);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->string, $this->style];
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
        return type_string();
    }

    public function eval(Row $row, FlowContext $context): ?string
    {
        $string = (new Parameter($this->string))->asString($row, $context);
        $style = (new Parameter($this->style))->as($row, $context, type_string(), type_enum(StringStyles::class));

        if ($string === null) {
            throw new InvalidArgumentException('StringStyle function requires non-null value');
        }

        if ($style === null) {
            throw new InvalidArgumentException('StringStyle function requires non-null style');
        }

        if (is_string($style)) {
            $style = StringStyles::fromString($style);
        }

        return $style->convert($string);
    }
}
