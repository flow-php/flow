<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row;

class RandomString implements ScalarFunction
{
    public function __construct(
        private readonly ScalarFunction|int $length,
        private readonly RandomValueGenerator $generator = new NativePHPRandomValueGenerator(),
    ) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $length = (new Parameter($this->length))->asInt($row, $context);

        if ($length === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('RandomString requires non-null length'));
        }

        return $this->generator->string($length);
    }
}
