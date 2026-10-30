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

final class StringTitle implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $string;
    private readonly ScalarFunction $allWords;

    public function __construct(ScalarFunction|string $string, ScalarFunction|bool $allWords = false)
    {
        $this->string = $string instanceof ScalarFunction ? $string : new Literal($string);
        $this->allWords = $allWords instanceof ScalarFunction ? $allWords : new Literal($allWords);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['string'] ?? null, ScalarFunction::class),
            $rebuild->of($data['all_words'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'string' => $this->string,
            'all_words' => $this->allWords,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->string, $this->allWords];
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
        $allWordsList = (new Parameter($this->allWords))->asBooleans($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($strings as $i => $string) {
                $allWords = $allWordsList[$i];

                $allWords ??= false;

                if ($string === null) {
                    throw new InvalidArgumentException('StringTitle function requires non-null value');
                }

                $results[] = u($string)->title(allWords: $allWords)->toString();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
