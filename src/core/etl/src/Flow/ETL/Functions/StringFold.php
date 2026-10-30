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

use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\u;

final class StringFold implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $string;

    public function __construct(ScalarFunction|string $string)
    {
        $this->string = $string instanceof ScalarFunction ? $string : new Literal($string);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['string'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'string' => $this->string,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->string];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0]);
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
        $results = [];
        $i = 0;

        try {
            foreach ($strings as $i => $string) {
                if ($string === null) {
                    throw new InvalidArgumentException('StringFold function requires non-null value');
                }

                $results[] = u($string)->folded()->toString();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
