<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};
use Symfony\Component\String\Slugger\AsciiSlugger;

final class Slug extends ScalarFunctionChain
{
    /**
     * @param ScalarFunction|string $string
     * @param ScalarFunction|string $separator
     * @param null|ScalarFunction|string $locale
     * @param null|array<array-key, mixed>|ScalarFunction $symbolsMap
     */
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|string $separator = '-',
        private readonly ScalarFunction|string|null $locale = null,
        private readonly ScalarFunction|array|null $symbolsMap = null,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?string
    {
        $string = (new Parameter($this->string))->asString($row, $context);
        $separator = (new Parameter($this->separator))->asString($row, $context, '-');
        $locale = (new Parameter($this->locale))->asString($row, $context);
        $symbolsMap = (new Parameter($this->symbolsMap))->asArray($row, $context);

        if ($string === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('Slug function requires non-null value'));
        }

        return (new AsciiSlugger(symbolsMap: $symbolsMap))->slug($string, $separator, $locale)->toString();
    }
}
