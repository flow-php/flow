<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

final class ModifyDateTime extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $reference,
        private readonly string|ScalarFunction $modifier,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $value = (new Parameter($this->reference))->asInstanceOf($row, $context, DateTimeInterface::class);
        $modifier = (new Parameter($this->modifier))->asString($row, $context);

        if ($modifier === null || $value === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('ModifyDateTime function requires non-null values'));
        }

        if (!$value instanceof DateTime && !$value instanceof DateTimeImmutable) {
            return $context
                ->functions()
                ->invalidResult(
                    new InvalidArgumentException(
                        'ModifyDateTime function requires DateTime or DateTimeImmutable object',
                    ),
                );
        }

        return $value->modify($modifier);
    }
}
