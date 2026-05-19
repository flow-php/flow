<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use InvalidArgumentException as BaseInvalidArgumentException;
use Symfony\Component\Uid\Ulid as SymfonyUlid;

use function class_exists;

if (!class_exists(SymfonyUlid::class)) {
    throw new RuntimeException(
        "\Symfony\Component\Uid\Ulid class not found, please add 'symfony/uid' as a dependency to the project first.",
    );
}

final class Ulid extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string|null $ref = null,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $param = (new Parameter($this->ref))->asString($row, $context);

        if (null !== $param) {
            try {
                return SymfonyUlid::fromString($param);
            } catch (BaseInvalidArgumentException $e) {
                return $context
                    ->functions()
                    ->invalidResult(
                        new InvalidArgumentException('Ulid requires valid ULID string: ' . $e->getMessage(), 0, $e),
                    );
            }
        }

        return new SymfonyUlid();
    }
}
