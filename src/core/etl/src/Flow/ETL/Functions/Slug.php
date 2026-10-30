<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;
use Symfony\Component\String\Slugger\AsciiSlugger;

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
        $this->string = $string instanceof ScalarFunction ? $string : new Literal($string);
        $this->separator = $separator instanceof ScalarFunction ? $separator : new Literal($separator);
        $this->locale = $locale instanceof ScalarFunction ? $locale : new Literal($locale);
        $this->symbolsMap = $symbolsMap instanceof ScalarFunction ? $symbolsMap : new Literal($symbolsMap);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['string'] ?? null, ScalarFunction::class),
            $rebuild->of($data['separator'] ?? null, ScalarFunction::class),
            $rebuild->of($data['locale'] ?? null, ScalarFunction::class),
            $rebuild->of($data['symbols_map'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'string' => $this->string,
            'separator' => $this->separator,
            'locale' => $this->locale,
            'symbols_map' => $this->symbolsMap,
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $strings = (new Parameter($this->string))->asStrings($rows, $backend);
        $separators = (new Parameter($this->separator))->asStrings($rows, $backend, '-');
        $locales = (new Parameter($this->locale))->asStrings($rows, $backend);
        $symbolsMaps = (new Parameter($this->symbolsMap))->asArrays($rows, $backend);
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

        return (new ResultColumn($backend))->of($this, $results);
    }
}
