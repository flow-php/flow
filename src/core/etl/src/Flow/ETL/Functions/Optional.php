<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\Evaluation\TolerantEvaluation;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function array_key_exists;
use function Flow\Types\DSL\type_optional;

final class Optional implements ScalarFunction
{
    use ScalarFunctionChain;

    public function __construct(
        private readonly ScalarFunction $function,
    ) {}

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->function];
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['function'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'function' => $this->function,
        ]);
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional($this->function->returns());
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $evaluated = (new TolerantEvaluation())->evaluate($this->function, $rows, $backend);
        $results = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $results[] = array_key_exists($i, $evaluated['failed']) ? null : $evaluated['values'][$i];
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
