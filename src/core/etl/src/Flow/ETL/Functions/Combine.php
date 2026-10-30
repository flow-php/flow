<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use SensitiveParameter;

use function array_combine;
use function array_is_list;
use function count;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_map;
use function is_int;
use function is_string;

final class Combine implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $keys;
    private readonly ScalarFunction $values;

    /**
     * @param array<array-key, mixed>|ScalarFunction $keys
     * @param array<array-key, mixed>|ScalarFunction $values
     */
    public function __construct(ScalarFunction|array $keys, ScalarFunction|array $values)
    {
        $this->keys = $keys instanceof ScalarFunction ? $keys : new Literal($keys);
        $this->values = $values instanceof ScalarFunction ? $values : new Literal($values);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['keys'] ?? null, ScalarFunction::class),
            $rebuild->of($data['values'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'keys' => $this->keys,
            'values' => $this->values,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->keys, $this->values];
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
        $keys = type_bare($this->keys->returns());
        $values = type_bare($this->values->returns());

        if (!$keys instanceof ListType || !$values instanceof ListType) {
            throw SchemaNotDerivableException::function('combine', 'both operands must declare list types');
        }

        // A key type outside the array-key template is refused at the Definition boundary.
        // @mago-expect analysis:template-constraint-violation
        // @mago-expect analysis:less-specific-nested-argument-type
        return type_map($keys->element(), $values->element());
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $keysList = (new Parameter($this->keys))->asArrays($rows, $backend);
        $valuesList = (new Parameter($this->values))->asArrays($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($keysList as $i => $keys) {
                $values = $valuesList[$i];

                if (null === $keys || null === $values) {
                    throw new InvalidArgumentException('Combine function requires non-null arrays');
                }

                if ([] === $keys) {
                    $results[] = [];

                    continue;
                }

                if (!array_is_list($keys)) {
                    throw new InvalidArgumentException('Combine function requires keys to be a list');
                }

                if (count($keys) !== count($values)) {
                    throw new InvalidArgumentException(
                        'Combine function requires keys and values arrays to have the same length',
                    );
                }

                if (!is_string($keys[0] ?? null) && !is_int($keys[0] ?? null)) {
                    throw new InvalidArgumentException('Combine function requires keys to be strings or integers');
                }

                /** @var array<array-key, array-key> $keys */
                $results[] = array_combine($keys, $values);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
