<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_datetime;

final class Now implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $timeZone;

    public function __construct(ScalarFunction|DateTimeZone $timeZone = new DateTimeZone('UTC'))
    {
        $this->timeZone = $timeZone instanceof ScalarFunction ? $timeZone : lit($timeZone);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->timeZone];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_datetime();
    }

    public function eval(Row $row, FlowContext $context): ?DateTimeImmutable
    {
        $tz = (new Parameter($this->timeZone))->asInstanceOf($row, $context, DateTimeZone::class);

        if ($tz === null) {
            throw new InvalidArgumentException('Now function requires valid DateTimeZone');
        }

        return new DateTimeImmutable('now', $tz);
    }
}
