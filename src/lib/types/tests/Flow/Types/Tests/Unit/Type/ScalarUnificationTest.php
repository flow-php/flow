<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Type;
use Flow\Types\Type\ScalarUnification;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

final class ScalarUnificationTest extends TestCase
{
    /**
     * @return Generator<string, array{Type<mixed>, Type<mixed>, ?Type<mixed>}>
     */
    public static function pairwise_cases(): Generator
    {
        yield 'identity' => [type_string(), type_string(), type_string()];

        yield 'null absorbs into the right side' => [type_null(), type_integer(), type_integer()];

        yield 'null absorbs into the left side' => [type_integer(), type_null(), type_integer()];

        yield 'null strips the other side to its bare type' => [
            type_null(),
            type_optional(type_integer()),
            type_integer(),
        ];

        yield 'integer and float widen to float' => [type_integer(), type_float(), type_float()];

        yield 'date and datetime widen to datetime' => [type_date(), type_datetime(), type_datetime()];

        yield 'string and integer are not a scalar arm' => [type_string(), type_integer(), null];

        yield 'boolean and integer have no common type' => [type_boolean(), type_integer(), null];

        yield 'containers are not handled here' => [type_list(type_integer()), type_list(type_float()), null];
    }

    #[DataProvider('pairwise_cases')]
    public function test_pairwise_unification(Type $left, Type $right, ?Type $expected): void
    {
        $result = (new ScalarUnification())->unify($left, $right);

        if ($expected === null) {
            static::assertNull($result);
        } else {
            static::assertNotNull($result);
            static::assertTrue(type_equals($expected, $result), $result->toString());
        }
    }
}
