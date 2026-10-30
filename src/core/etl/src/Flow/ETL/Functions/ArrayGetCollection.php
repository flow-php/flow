<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ArrayDot\Path;
use Flow\ArrayDot\Step\Key;
use Flow\ArrayDot\Step\Multimatch;
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

use function array_map;
use function array_values;
use function Flow\ArrayDot\array_dot_get;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_optional;
use function get_debug_type;
use function is_array;
use function is_scalar;
use function sprintf;

final class ArrayGetCollection implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $keys;
    private readonly ScalarFunction $index;

    /**
     * @param ScalarFunction|array<array-key, mixed> $keys
     */
    public function __construct(
        private readonly ScalarFunction $ref,
        ScalarFunction|array $keys,
        ScalarFunction|string $index = '*',
    ) {
        $this->keys = $keys instanceof ScalarFunction ? $keys : new Literal($keys);
        $this->index = $index instanceof ScalarFunction ? $index : new Literal($index);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['ref'] ?? null, ScalarFunction::class),
            $rebuild->of($data['keys'] ?? null, ScalarFunction::class),
            $rebuild->of($data['index'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'ref' => $this->ref,
            'keys' => $this->keys,
            'index' => $this->index,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->ref, $this->keys, $this->index];
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
        return type_optional(type_array());
    }

    /**
     * @param ScalarFunction|array<string> $keys
     */
    public static function fromFirst(ScalarFunction $ref, ScalarFunction|array $keys): self
    {
        return new self($ref, $keys, '0');
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        try {
            $values = (new Parameter($this->ref))->asArrays($rows, $backend);
            $indexes = (new Parameter($this->index))->asStrings($rows, $backend);
            $keysList = (new Parameter($this->keys))->asArrays($rows, $backend);
        } catch (EvaluationException $e) {
            throw new EvaluationException(
                $e->rowIndex,
                new InvalidArgumentException('ArrayGetCollection function failed to evaluate parameters.', 0, $e),
            );
        }

        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $index = $indexes[$i];
                $keys = $keysList[$i];

                try {
                    if ($value === null || $index === null || $keys === null) {
                        throw new InvalidArgumentException(
                            'ArrayGetCollection function requires non-null array, index, and keys',
                        );
                    }

                    // the keys are literal, so they become Key steps and are never parsed as a path
                    $keyPaths = array_map(static function (mixed $key): Path {
                        if (!is_scalar($key)) {
                            throw new InvalidArgumentException(sprintf(
                                'ArrayGetCollection keys must be scalar, got "%s".',
                                get_debug_type($key),
                            ));
                        }

                        return new Path([new Key((string) $key, nullsafe: true)]);
                    }, array_values($keys));

                    try {
                        // @mago-ignore analysis:mixed-assignment
                        $extractedValues = array_dot_get($index === '0' ? array_values($value) : $value, new Path([
                            ...Path::fromString($index)->steps,
                            new Multimatch($keyPaths),
                        ]));
                    } catch (InvalidPathException $e) {
                        throw new InvalidArgumentException(
                            'ArrayGetCollection function failed to get values from array.',
                            0,
                            $e,
                        );
                    }

                    if (!is_array($extractedValues)) {
                        throw new InvalidArgumentException(
                            'ArrayGetCollection function requires the result to be an array',
                        );
                    }

                    $results[] = $extractedValues;
                } catch (InvalidArgumentException $e) {
                    throw new InvalidArgumentException(
                        'ArrayGetCollection function failed to evaluate parameters.',
                        0,
                        $e,
                    );
                }
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
