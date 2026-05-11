<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Symfony\Component\String\u;

final class StringNormalize extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|int $form = \Normalizer::NFC,
    ) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $form = (new Parameter($this->form))->asInt($row, $context, \Normalizer::NFC);

        if ($value === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StringNormalize function requires non-null value'));
        }

        return u($value)->normalize($form)->toString();
    }
}
