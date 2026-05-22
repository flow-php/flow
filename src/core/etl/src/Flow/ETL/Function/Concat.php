<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function implode;
use function is_string;

final class Concat extends ScalarFunctionChain
{
    /**
     * @var array<ScalarFunction|string>
     */
    private readonly array $refs;

    public function __construct(ScalarFunction|string ...$refs)
    {
        $this->refs = $refs;
    }

    public function eval(Row $row, FlowContext $context): string
    {
        /** @var array<string> $concatValues */
        $concatValues = [];

        foreach ($this->refs as $value) {
            $value = is_string($value)
                ? $value
                : type_optional(type_string())->cast((new Parameter($value))->eval($row, $context));

            // @mago-ignore analysis:redundant-condition,redundant-type-comparison
            if (is_string($value)) {
                $concatValues[] = $value;
            }
        }

        return implode('', $concatValues);
    }
}
