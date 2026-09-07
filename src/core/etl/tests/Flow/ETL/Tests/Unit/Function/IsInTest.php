<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidArgumentException;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class IsInTest extends FlowTestCase
{
    public function test_is_in_accepts_a_container_haystack_without_a_single_element_type(): void
    {
        // Parameter::asArray() iterates a structure, a map and a bare array alike, so the bind gate
        // must not refuse them - only a list and a map declare one element type to compare against.
        static::assertSame(
            'boolean',
            lit('a')
                ->isIn(lit([1, 'a']))
                ->returns()
                ->toString(),
        );
        static::assertTrue(lit('a')->isIn(lit([1, 'a']))->eval(row([]), flow_context()));
    }

    public function test_is_in_matches_two_equal_datetimes(): void
    {
        // in_array(strict: true) compared these by identity and answered false
        static::assertTrue(
            lit(new DateTimeImmutable('2024-01-01 10:00:00'))
                ->isIn(lit([new DateTimeImmutable('2024-01-01 10:00:00')]))
                ->eval(row([]), flow_context()),
        );
    }

    public function test_is_in_does_not_match_two_equal_date_intervals(): void
    {
        // Equals::eval() has a DateInterval arm and IsIn deliberately does not
        static::assertTrue(
            lit(new DateInterval('PT1H'))->equals(lit(new DateInterval('PT60M')))->eval(row([]), flow_context()),
        );
        static::assertFalse(
            lit(new DateInterval('PT1H'))->isIn(lit([new DateInterval('PT60M')]))->eval(row([]), flow_context()),
        );
    }

    public function test_is_in_over_an_empty_haystack(): void
    {
        static::assertSame('boolean', lit('a')->isIn(lit([]))->returns()->toString());
        static::assertFalse(lit('a')->isIn(lit([]))->eval(row([]), flow_context()));
    }

    public function test_is_in_agrees_with_equals_on_numeric_operands(): void
    {
        $row = row(['a' => '1']);

        static::assertTrue(ref('a')->equals(lit(1))->eval($row, flow_context()));
        static::assertTrue(ref('a')->isIn(lit([1]))->eval($row, flow_context()));
    }

    public function test_is_in_refuses_an_incomparable_element_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't compare '(string == date)' due to data type mismatch.");

        lit('a')->isIn(lit([new DateTimeImmutable('2024-01-01')]))->returns();
    }

    public function test_a_match_beats_a_null_element(): void
    {
        static::assertTrue(lit(5)->isIn(lit([1, null, 5]))->eval(row([]), flow_context()));
    }

    public function test_no_match_with_a_null_element_is_null(): void
    {
        static::assertNull(lit(7)->isIn(lit([1, null, 5]))->eval(row([]), flow_context()));
    }

    public function test_no_match_without_a_null_element_is_false(): void
    {
        static::assertFalse(lit(7)->isIn(lit([1, 3, 5]))->eval(row([]), flow_context()));
    }

    public function test_a_null_needle_is_null(): void
    {
        static::assertNull(lit(null)->isIn(lit([1, 3, 5]))->eval(row([]), flow_context()));
    }

    public function test_a_null_haystack_is_null(): void
    {
        static::assertNull(lit(5)->isIn(lit(null))->eval(row(['x' => []]), flow_context()));
    }
}
