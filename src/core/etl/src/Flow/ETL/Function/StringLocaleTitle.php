<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\type_string;
use function Symfony\Component\String\u;
use Flow\ETL\Function\ScalarFunction\TypedScalarFunction;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row;

final class StringLocaleTitle extends ScalarFunctionChain implements TypedScalarFunction
{
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|string $locale,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $string = (new Parameter($this->string))->asString($row);
        $locale = (new Parameter($this->locale))->asString($row);

        if ($string === null || $locale === null) {
            return null;
        }

        return u($string)->localeTitle($locale)->toString();
    }

    public function returns() : Type
    {
        return type_string();
    }
}
