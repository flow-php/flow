<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Flow\Types\DSL\type_integer;
use function Symfony\Component\String\u;

final class IndexOf extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|string $needle,
        private readonly ScalarFunction|bool $ignoreCase = false,
        private readonly ScalarFunction|int $offset = 0,
    ) {}

    public function eval(Row $row, FlowContext $context): int|false|null
    {
        $string = (new Parameter($this->string))->asString($row, $context);
        $needle = (new Parameter($this->needle))->asString($row, $context);
        $offset = type_integer()->assert((new Parameter($this->offset))->as($row, $context, type_integer()));
        $ignoreCase = (new Parameter($this->ignoreCase))->asBoolean($row, $context);

        if ($string === null || $needle === null) {
            $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('IndexOf function requires non-null string and needle'));

            return false;
        }

        if ($ignoreCase) {
            return u($string)->ignoreCase()->indexOf($needle, $offset);
        }

        return u($string)->indexOf($needle, $offset);
    }
}
