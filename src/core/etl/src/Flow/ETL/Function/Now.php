<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class Now extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|\DateTimeZone $timeZone = new \DateTimeZone('UTC'))
    {
    }

    public function eval(Row $row, FlowContext $context) : ?\DateTimeImmutable
    {
        $tz = (new Parameter($this->timeZone))->asInstanceOf($row, $context, \DateTimeZone::class);

        if ($tz === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('Now function requires valid DateTimeZone'));
        }

        return new \DateTimeImmutable('now', $tz);
    }
}
