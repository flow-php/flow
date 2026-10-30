<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\ScalarFunction\ExpandResults;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

use function array_keys;
use function array_map;
use function array_values;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final class ArrayExpand implements ScalarFunction, ExpandResults
{
    use ScalarFunctionChain;

    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ArrayExpand\ArrayExpand $expand,
    ) {}

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->ref];
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['ref'] ?? null, ScalarFunction::class),
            type_enum(ArrayExpand\ArrayExpand::class)->assert(constant(
                ArrayExpand\ArrayExpand::class . '::' . type_string()->assert($data['expand'] ?? null),
            )),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'ref' => $this->ref,
            'expand' => $this->expand->name,
        ]);
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->expand);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $array = type_bare($this->ref->returns());

        if (!$array instanceof ListType && !$array instanceof MapType && !$array instanceof StructureType) {
            throw SchemaNotDerivableException::function(
                'array_expand',
                'the array operand declares "' . $array->toString() . '", which has no element type',
            );
        }

        // The unification can refuse, so KEYS must not ask for the value type it does not declare.
        $value = fn(): Type => match (true) {
            $array instanceof ListType => $array->element(),
            $array instanceof MapType => $array->value(),
            default => StructureValues::type('array_expand', $array),
        };

        $key = match (true) {
            $array instanceof ListType => type_integer(),
            $array instanceof MapType => $array->key(),
            default => type_string(),
        };

        return match ($this->expand) {
            ArrayExpand\ArrayExpand::VALUES => $value(),
            ArrayExpand\ArrayExpand::KEYS => $key,
            ArrayExpand\ArrayExpand::BOTH => type_map($key, $value()),
        };
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $lists = [];

        foreach ((new Parameter($this->ref))->asArrays($rows, $backend) as $array) {
            if ($array === null) {
                $lists[] = [];

                continue;
            }

            $lists[] = match ($this->expand) {
                ArrayExpand\ArrayExpand::KEYS => array_keys($array),
                ArrayExpand\ArrayExpand::BOTH => array_map(
                    static fn($key, $value) => [$key => $value],
                    array_keys($array),
                    $array,
                ),
                default => array_values($array),
            };
        }

        return (new ResultColumn($backend))->lists($this, $lists);
    }
}
