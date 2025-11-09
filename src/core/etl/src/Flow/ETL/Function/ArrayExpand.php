<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};
use Flow\ETL\Function\ScalarFunction\ExpandResults;

final class ArrayExpand extends ScalarFunctionChain implements ExpandResults
{
    public function __construct(private readonly ScalarFunction $ref, private readonly ArrayExpand\ArrayExpand $expand)
    {
    }

    /**
     * @return array<mixed>
     */
    public function eval(Row $row, FlowContext $context) : array
    {
        $array = (new Parameter($this->ref))->asArray($row, $context);

        if ($array === null) {
            $context->functions()->invalidResult(new InvalidArgumentException('ArrayExpand requires non-null array'));

            return [];
        }

        if ($this->expand === ArrayExpand\ArrayExpand::KEYS) {
            return \array_keys($array);
        }

        if ($this->expand === ArrayExpand\ArrayExpand::BOTH) {
            return \array_map(static fn ($key, $value) => [$key => $value], \array_keys($array), $array);
        }

        return $array;
    }
}
