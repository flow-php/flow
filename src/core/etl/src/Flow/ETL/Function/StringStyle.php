<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $strings = (new Parameter($this->string))->asStrings($rows, $context);
        $styles = (new Parameter($this->style))->asTypes(
            $rows,
            $context,
            type_string(),
            type_enum(StringStyles::class),
        );
        $results = [];
        $i = 0;

        try {
            foreach ($strings as $i => $string) {
                $style = $styles[$i];

                if ($string === null) {
                    throw new InvalidArgumentException('StringStyle function requires non-null value');
                }

                if ($style === null) {
                    throw new InvalidArgumentException('StringStyle function requires non-null style');
                }

                if (is_string($style)) {
                    $style = StringStyles::fromString($style);
                }

                $results[] = $style->convert($string);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
