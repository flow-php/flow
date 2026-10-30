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

use function Flow\Types\DSL\type_array;
use function preg_match_all;

use const PREG_SET_ORDER;

final class StringMatchAll implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $haystack;
    private readonly ScalarFunction $pattern;

    public function __construct(ScalarFunction|string $haystack, ScalarFunction|string $pattern)
    {
        $this->haystack = $haystack instanceof ScalarFunction ? $haystack : new Literal($haystack);
        $this->pattern = $pattern instanceof ScalarFunction ? $pattern : new Literal($pattern);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['haystack'] ?? null, ScalarFunction::class),
            $rebuild->of($data['pattern'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'haystack' => $this->haystack,
            'pattern' => $this->pattern,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->haystack, $this->pattern];
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
        return type_array();
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $haystacks = (new Parameter($this->haystack))->asStrings($rows, $backend);
        $patterns = (new Parameter($this->pattern))->asStrings($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($haystacks as $i => $haystack) {
                $pattern = $patterns[$i];

                if ($haystack === null) {
                    throw new InvalidArgumentException('StringMatchAll function requires non-null haystack');
                }

                if ($pattern === null) {
                    throw new InvalidArgumentException('StringMatchAll function requires non-null pattern');
                }

                try {
                    $matches = [];

                    if (preg_match_all($pattern, $haystack, $matches, PREG_SET_ORDER) !== false) {
                        $results[] = $matches;

                        continue;
                    }

                    $results[] = [];
                } catch (Exception $e) {
                    throw new InvalidArgumentException('StringMatchAll error: ' . $e->getMessage(), 0, $e);
                }
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
