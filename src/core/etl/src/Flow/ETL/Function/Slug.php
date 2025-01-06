<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;
use Symfony\Component\String\Slugger\AsciiSlugger;

final class Slug extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|string $separator = '-',
        private readonly ScalarFunction|string|null $locale = null,
        private readonly ScalarFunction|array|null $symbolsMap = null,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $string = (new Parameter($this->string))->asString($row);
        $separator = (new Parameter($this->separator))->asString($row);
        $locale = (new Parameter($this->locale))->asString($row);
        $symbolsMap = (new Parameter($this->symbolsMap))->asArray($row);

        if ($string === null) {
            return null;
        }

        return (new AsciiSlugger(symbolsMap: $symbolsMap))->slug($string, $separator, $locale)->toString();
    }
}
