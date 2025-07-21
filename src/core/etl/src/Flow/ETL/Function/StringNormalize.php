<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\u;
use Flow\ETL\Row;

final class StringNormalize extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|int $form = \Normalizer::NFC,
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $value = (new Parameter($this->value))->asString($row);
        $form = (new Parameter($this->form))->asInt($row, \Normalizer::NFC);

        if ($value === null) {
            return null;
        }

        return u($value)->normalize($form)->toString();
    }
}
