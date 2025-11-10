<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class Chunk extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|int $size,
    ) {
    }

    /**
     * @return null|array<int, string>
     */
    public function eval(Row $row, FlowContext $context) : ?array
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $size = (new Parameter($this->size))->asInt($row, $context);

        if ($value === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('Chunk function requires non-null value'));
        }

        if ($size === null || $size <= 0) {
            $context->functions()->invalidResult(new InvalidArgumentException('Chunk function requires non-null, positive size'));

            return [];
        }

        $chunks = s($value)->chunk($size);

        return array_map(static fn ($chunk) => $chunk->toString(), iterator_to_array($chunks));
    }
}
