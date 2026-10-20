<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter;

use DateTimeImmutable;
use Flow\Parquet\Filter\Literal;
use Flow\Parquet\Tests\Double\StringableUuid;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use const INF;

final class LiteralTest extends TestCase
{
    #[TestWith([1])]
    #[TestWith([1.5])]
    #[TestWith([INF])]
    #[TestWith(['a'])]
    #[TestWith([true])]
    public function test_a_scalar_is_kept_as_it_is(bool|float|int|string $value): void
    {
        static::assertSame($value, Literal::of($value));
    }

    public function test_a_stringable_is_kept_as_its_string(): void
    {
        static::assertSame(
            'f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a',
            Literal::of(new StringableUuid('f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a')),
        );
    }

    public function test_an_immutable_date_time_is_kept_equal(): void
    {
        $date = new DateTimeImmutable('2024-01-01 00:00:00 UTC');

        static::assertEquals($date, Literal::of($date));
    }

    public function test_another_kind_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'A filter literal must be a scalar, a DateTimeInterface or a Stringable, array given',
        );

        Literal::of([1]);
    }
}
