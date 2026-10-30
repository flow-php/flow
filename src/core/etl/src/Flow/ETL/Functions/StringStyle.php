<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\String\StringStyles;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_string;

final class StringStyle implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $string;
    private readonly ScalarFunction $style;

    public function __construct(ScalarFunction|string $string, ScalarFunction|string|StringStyles $style)
    {
        $this->string = $string instanceof ScalarFunction ? $string : new Literal($string);
        $this->style = $style instanceof ScalarFunction ? $style : new Literal($style);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['string'] ?? null, ScalarFunction::class),
            $rebuild->of($data['style'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'string' => $this->string,
            'style' => $this->style,
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $strings = (new Parameter($this->string))->asStrings($rows, $backend);
        $styles = (new Parameter($this->style))->asTypes(
            $rows,
            $backend,
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

        return (new ResultColumn($backend))->of($this, $results);
    }
}
