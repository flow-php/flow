<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Kernels\Comparison\ValueEquality;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Nullability;
use Flow\Types\Type\ValueComparator;
use SensitiveParameter;

use function Flow\Types\DSL\type_boolean;

final class IsIn implements ComparisonFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $haystack;
    private readonly ScalarFunction $needle;

    /**
     * @param array<array-key, mixed>|ScalarFunction $haystack
     */
    public function __construct(ScalarFunction|array $haystack, mixed $needle)
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
    public function operands(): array
    {
        return [$this->haystack, $this->needle];
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
     * @param list<ScalarFunction> $operands
     */
    public function withOperands(array $operands): static
    {
        return new self($operands[0], $operands[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $haystack = $this->haystack->returns();
        $needle = $this->needle->returns();
        $nullability = new Nullability();
        $bare = $nullability->bare($haystack);

        // structure, json and array<mixed> declare no element type
        $declaredElement = match (true) {
            $bare instanceof ListType => $nullability->bare($bare->element()),
            $bare instanceof MapType => $nullability->bare($bare->value()),
            default => null,
        };

        if ($declaredElement !== null) {
            (new ValueComparator())->assertComparableTypes($nullability->bare($needle), $declaredElement, '==');
        }

        return $nullability->any(type_boolean(), $haystack, $needle);
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        return (new ResultColumn($backend))->of($this, (new ValueEquality())->in(
            $this->needle->eval($rows, $backend),
            (new Parameter($this->haystack))->arraysOf($this->haystack->eval($rows, $backend)),
            $this->needle instanceof Reference ? $this->needle->to() : 'needle',
            $this->haystack instanceof Reference ? $this->haystack->to() : 'haystack',
        ));
    }
}
