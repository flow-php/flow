<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;
use Flow\Types\Type\TypeFactory;
use SensitiveParameter;

use function array_map;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function is_string;

final class IsType implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var list<Type<mixed>>
     */
    private readonly array $types;

    private readonly ScalarFunction $value;

    /**
     * @param string|Type<mixed> ...$types
     */
    public function __construct(mixed $value, string|Type ...$types)
    {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->types = array_values(array_map(static fn(string|Type $type): Type => is_string($type)
            ? TypeFactory::fromString($type)
            : $type, $types));
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            ...array_map(static fn(array $type): Type => type_from_array(type_map(type_string(), type_mixed())->assert(
                $type,
            )), type_list(type_array())->assert($data['types'] ?? null)),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'types' => array_map(static fn(Type $type): array => $type->normalize(), $this->types),
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], ...$this->types);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return (new Nullability())->any(type_boolean(), $this->value->returns());
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $results = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ((new Parameter($this->value))->values($rows, $backend) as $value) {
            // A NULL operand is unknowable, even against type_null() - ->isNull() is the null test.
            if ($value === null) {
                $results[] = null;

                continue;
            }

            foreach ($this->types as $type) {
                if ($type->isValid($value)) {
                    $results[] = true;

                    continue 2;
                }
            }

            $results[] = false;
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
