<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\{FlowContext, Row};

if (!\class_exists(\Symfony\Component\Uid\Ulid::class)) {
    throw new RuntimeException("\Symfony\Component\Uid\Ulid class not found, please add 'symfony/uid' as a dependency to the project first.");
}

final class Ulid extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|string|null $ref = null)
    {
    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        $param = (new Parameter($this->ref))->asString($row, $context);

        if (null !== $param) {
            try {
                return \Symfony\Component\Uid\Ulid::fromString($param);
            } catch (\InvalidArgumentException $e) {
                return $context->functions()->invalidResult(new InvalidArgumentException('Ulid requires valid ULID string: ' . $e->getMessage(), 0, $e));
            }
        }

        return new \Symfony\Component\Uid\Ulid();
    }
}
