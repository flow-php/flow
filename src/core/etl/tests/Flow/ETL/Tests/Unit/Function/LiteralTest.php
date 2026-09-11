<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\Literal;
use Flow\ETL\Tests\FlowTestCase;

final class LiteralTest extends FlowTestCase
{
    public function test_constructor_rejects_a_closure_nested_in_an_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'A Closure cannot be used as a literal value: pipeline objects must not hold executable state.',
        );

        new Literal(['handlers' => [static fn(): int => 1]]);
    }

    public function test_constructor_rejects_a_closure(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'A Closure cannot be used as a literal value: pipeline objects must not hold executable state.',
        );

        new Literal(static fn(): int => 1);
    }
}
