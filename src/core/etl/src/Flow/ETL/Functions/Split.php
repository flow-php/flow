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
use Symfony\Component\String\AbstractString;

use function array_map;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\s;

final class Split implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $separator;
    private readonly ScalarFunction $limit;

    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|string $separator,
        ScalarFunction|int $limit = PHP_INT_MAX,
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->separator = $separator instanceof ScalarFunction ? $separator : new Literal($separator);
        $this->limit = $limit instanceof ScalarFunction ? $limit : new Literal($limit);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            $rebuild->of($data['separator'] ?? null, ScalarFunction::class),
            $rebuild->of($data['limit'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'separator' => $this->separator,
            'limit' => $this->limit,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->separator, $this->limit];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_list(type_string());
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $values = (new Parameter($this->value))->asStrings($rows, $backend);
        $separators = (new Parameter($this->separator))->asStrings($rows, $backend);
        $limits = (new Parameter($this->limit))->asInts($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $separator = $separators[$i];
                $limit = $limits[$i];

                if ($value === null) {
                    throw new InvalidArgumentException('Split function requires non-null value');
                }

                if ($separator === null || $limit === null || $separator === '') {
                    throw new InvalidArgumentException(
                        'Split function requires non-null separator and limit, separator cannot be empty',
                    );
                }

                $results[] = array_map(
                    static fn(AbstractString $s): string => $s->toString(),
                    s($value)->split($separator, $limit),
                );
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
