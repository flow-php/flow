<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\{type_callable};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

final class CallableTypeTest extends FlowTestCase
{
    #[TestWith(['some_string', 'Expected type "callable", got "string"'])]
    public function test_invalid_assertion(mixed $value, string $exception) : void
    {
        $this->expectExceptionMessage($exception);

        type_callable()->assert($value);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'callable',
            type_callable()->toString()
        );

    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_callable()->isValid('printf')
        );

        self::assertFalse(
            type_callable()->isValid('one')
        );
        self::assertFalse(
            type_callable()->isValid([1, 2])
        );
        self::assertFalse(
            type_callable()->isValid(123)
        );
    }

    #[TestWith(['count'])]
    public function test_valid_assertion(mixed $value) : void
    {
        self::assertIsCallable(type_callable()->assert($value));
    }
}
