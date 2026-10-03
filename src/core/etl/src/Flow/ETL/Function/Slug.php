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
use Flow\Types\Type;
use Symfony\Component\String\Slugger\AsciiSlugger;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;

final class Slug implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $string;
    private readonly ScalarFunction $separator;
    private readonly ScalarFunction $locale;
    private readonly ScalarFunction $symbolsMap;

    /**
     * @param null|array<array-key, mixed>|ScalarFunction $symbolsMap
     */
    public function __construct(
        ScalarFunction|string $string,
        ScalarFunction|string $separator = '-',
        ScalarFunction|string|null $locale = null,
        ScalarFunction|array|null $symbolsMap = null,
    ) {
        $this->string = $string instanceof ScalarFunction ? $string : lit($string);
        $this->separator = $separator instanceof ScalarFunction ? $separator : lit($separator);
        $this->locale = $locale instanceof ScalarFunction ? $locale : lit($locale);
        $this->symbolsMap = $symbolsMap instanceof ScalarFunction ? $symbolsMap : lit($symbolsMap);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->string, $this->separator, $this->locale, $this->symbolsMap];
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $strings = (new Parameter($this->string))->asStrings($rows, $context);
        $separators = (new Parameter($this->separator))->asStrings($rows, $context, '-');
        $locales = (new Parameter($this->locale))->asStrings($rows, $context);
        $symbolsMaps = (new Parameter($this->symbolsMap))->asArrays($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($strings as $i => $string) {
                $separator = $separators[$i];
                $locale = $locales[$i];
                $symbolsMap = $symbolsMaps[$i];

                if ($string === null) {
                    throw new InvalidArgumentException('Slug function requires non-null value');
                }

                $results[] = (new AsciiSlugger(symbolsMap: $symbolsMap))
                    ->slug($string, $separator, $locale)
                    ->toString();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
