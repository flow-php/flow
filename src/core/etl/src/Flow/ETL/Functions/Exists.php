<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\Evaluation\TolerantEvaluation;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function array_fill;
use function array_key_exists;
use function Flow\Types\DSL\type_boolean;

final class Exists implements ScalarFunction
{
    use ScalarFunctionChain;

    public function __construct(
        private readonly ScalarFunction $ref,
    ) {}

    /**
     * "Can this reference be reached" is the whole question - the operand must not resolve
     * against the schema, or ref('missing')->exists() would be refused by the gate.
     *
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [];
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['ref'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'ref' => $this->ref,
        ]);
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return $this;
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_boolean();
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        if ($this->ref instanceof Reference) {
            $exists = $rows->schema()->findDefinition($this->ref->name()) !== null;

            // @mago-ignore analysis:possibly-invalid-argument
            return (new ResultColumn($backend))->of(
                $this,
                $rows->isEmpty() ? [] : array_fill(0, $rows->count(), $exists),
            );
        }

        // the operand is left unresolved by the gate (children() is empty): a reference the batch lacks stays
        // unresolved and throws, which is this function's "no"
        $failed = (new TolerantEvaluation())->evaluate(
            (new ReferenceResolver())->resolve($this->ref, $rows->schema()),
            $rows,
            $backend,
        )['failed'];
        $results = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $results[] = !array_key_exists($i, $failed);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
