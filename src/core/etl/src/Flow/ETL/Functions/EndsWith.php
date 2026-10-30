<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;
use SensitiveParameter;

use function Flow\Types\DSL\type_boolean;
use function str_ends_with;

final class EndsWith implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $haystack;
    private readonly ScalarFunction $needle;

    public function __construct(ScalarFunction|string $haystack, ScalarFunction|string $needle)
    {
        $this->haystack = $haystack instanceof ScalarFunction ? $haystack : new Literal($haystack);
        $this->needle = $needle instanceof ScalarFunction ? $needle : new Literal($needle);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['haystack'] ?? null, ScalarFunction::class),
            $rebuild->of($data['needle'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'haystack' => $this->haystack,
            'needle' => $this->needle,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->haystack, $this->needle];
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
        return (new Nullability())->any(type_boolean(), $this->haystack->returns(), $this->needle->returns());
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $haystacks = (new Parameter($this->haystack))->asStrings($rows, $backend);
        $needles = (new Parameter($this->needle))->asStrings($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($haystacks as $i => $haystack) {
                $needle = $needles[$i];

                if ($haystack === null || $needle === null) {
                    $results[] = null;

                    continue;
                }

                $results[] = str_ends_with($haystack, $needle);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
