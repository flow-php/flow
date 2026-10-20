<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\LiteralDomain;
use Flow\Parquet\Filter\Binding\PreImage\BooleanPreImage;
use Flow\Parquet\Filter\Binding\PreImage\BytesPreImage;
use Flow\Parquet\Filter\Binding\PreImage\DatePreImage;
use Flow\Parquet\Filter\Binding\PreImage\DecimalPreImage;
use Flow\Parquet\Filter\Binding\PreImage\DoublePreImage;
use Flow\Parquet\Filter\Binding\PreImage\Float32PreImage;
use Flow\Parquet\Filter\Binding\PreImage\IntegerPreImage;
use Flow\Parquet\Filter\Binding\PreImage\TimestampPreImage;
use Flow\Parquet\Filter\Binding\PreImage\UnsignedInt64PreImage;
use Flow\Parquet\Filter\Binding\PreImage\UuidPreImage;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use PHPUnit\Framework\TestCase;

use const NAN;
use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class LiteralDomainTest extends TestCase
{
    public function test_each_column_kind_maps_to_the_domain_and_pre_image_of_its_decoded_values(): void
    {
        $of = static fn(FlatColumn $column): array => LiteralDomain::forColumn(
            $column,
            DataConverter::initialize(new Options())->resolveConverter($column),
        );

        static::assertEquals([LiteralDomain::Boolean, new BooleanPreImage()], $of(FlatColumn::boolean('b')));
        static::assertEquals(
            [LiteralDomain::Number, new IntegerPreImage(PHP_INT_MIN, PHP_INT_MAX)],
            $of(FlatColumn::int64('i')),
        );
        static::assertEquals(
            [LiteralDomain::Number, new IntegerPreImage(-2_147_483_648, 2_147_483_647)],
            $of(FlatColumn::int32('i')),
        );
        static::assertEquals([LiteralDomain::Number, new IntegerPreImage(0, 255)], $of(FlatColumnMother::uint8()));
        static::assertEquals(
            [LiteralDomain::Number, new IntegerPreImage(0, 4_294_967_295)],
            $of(FlatColumnMother::uint32()),
        );
        static::assertEquals([LiteralDomain::Number, new DoublePreImage()], $of(FlatColumn::double('d')));
        static::assertEquals([LiteralDomain::Number, new Float32PreImage()], $of(FlatColumn::float('f')));
        static::assertEquals(
            [LiteralDomain::Number, DecimalPreImage::forColumn(FlatColumn::decimal('m', 10, 2))],
            $of(FlatColumn::decimal('m', 10, 2)),
        );
        static::assertSame(LiteralDomain::UnsignedInteger64, $of(FlatColumnMother::uint64())[0]);
        static::assertInstanceOf(UnsignedInt64PreImage::class, $of(FlatColumnMother::uint64())[1]);
        static::assertEquals([LiteralDomain::Bytes, new BytesPreImage()], $of(FlatColumn::string('s')));
        static::assertEquals([LiteralDomain::Bytes, new BytesPreImage()], $of(FlatColumn::enum('e')));
        static::assertEquals([LiteralDomain::Bytes, null], $of(FlatColumn::json('j')));
        static::assertSame(LiteralDomain::Uuid, $of(FlatColumn::uuid('u'))[0]);
        static::assertInstanceOf(UuidPreImage::class, $of(FlatColumn::uuid('u'))[1]);
        static::assertEquals([LiteralDomain::DateTime, new DatePreImage()], $of(FlatColumn::date('day')));
        static::assertEquals(
            [LiteralDomain::DateTime, new TimestampPreImage(TimeUnit::MICROSECONDS)],
            $of(FlatColumn::dateTime('ts')),
        );
        static::assertEquals([LiteralDomain::None, null], $of(FlatColumn::time('t')));
        static::assertEquals([LiteralDomain::None, null], $of(FlatColumnMother::float16()));
        static::assertEquals([LiteralDomain::None, null], $of(FlatColumnMother::interval()));
        static::assertEquals([LiteralDomain::Number, null], $of(FlatColumnMother::byteArrayDecimal()));
        static::assertEquals([LiteralDomain::None, null], LiteralDomain::forColumn(FlatColumnMother::int96(), null));
    }

    public function test_a_domain_holds_only_the_literals_its_values_compare_with(): void
    {
        static::assertTrue(LiteralDomain::Boolean->holds(true));
        static::assertFalse(LiteralDomain::Boolean->holds(1));
        static::assertTrue(LiteralDomain::Number->holds(2.5));
        static::assertFalse(LiteralDomain::Number->holds('1'));
        static::assertTrue(LiteralDomain::UnsignedInteger64->holds('18446744073709551616'));
        static::assertTrue(LiteralDomain::UnsignedInteger64->holds('0'));
        static::assertTrue(LiteralDomain::UnsignedInteger64->holds(-1));
        static::assertTrue(LiteralDomain::UnsignedInteger64->holds(1.5));
        static::assertFalse(LiteralDomain::UnsignedInteger64->holds(' 9'));
        static::assertFalse(LiteralDomain::UnsignedInteger64->holds('1e1'));
        static::assertFalse(LiteralDomain::UnsignedInteger64->holds('09'));
        static::assertTrue(LiteralDomain::Uuid->holds('00000000-0000-4000-8000-00000000000a'));
        static::assertFalse(LiteralDomain::Uuid->holds('00000000-0000-4000-8000-00000000000A'));
        static::assertTrue(LiteralDomain::DateTime->holds(new DateTimeImmutable()));
        static::assertFalse(LiteralDomain::DateTime->holds('2024-01-01'));
        static::assertFalse(LiteralDomain::None->holds('x'));
        static::assertTrue(LiteralDomain::Uuid->holdsPrefix());
        static::assertSame(
            '00000000-0000-4000-8000-0000000000aa',
            LiteralDomain::Uuid->normalise('00000000-0000-4000-8000-0000000000AA'),
        );
        static::assertSame('ABC', LiteralDomain::Bytes->normalise('ABC'));
        static::assertFalse(LiteralDomain::Number->holdsPrefix());
    }

    public function test_values_equal_by_the_domain_share_a_key(): void
    {
        static::assertSame(LiteralDomain::Number->key(1), LiteralDomain::Number->key(1.0));
        static::assertSame(LiteralDomain::Number->key(0), LiteralDomain::Number->key(-0.0));
        static::assertSame(2 ** 53, LiteralDomain::Number->key((float) 2 ** 53));
        static::assertNotSame(LiteralDomain::Number->key(1), LiteralDomain::Number->key(1.5));
        static::assertNull(LiteralDomain::Number->key(NAN));
        static::assertSame(
            LiteralDomain::DateTime->key(new DateTimeImmutable('2024-01-01 02:00 +02:00')),
            LiteralDomain::DateTime->key(new DateTimeImmutable('2024-01-01 00:00 UTC')),
        );
        static::assertSame(LiteralDomain::UnsignedInteger64->key(10), LiteralDomain::UnsignedInteger64->key('10'));
        static::assertSame('a', LiteralDomain::Bytes->key('a'));
    }

    public function test_a_value_ordered_rather_than_hashed_has_no_key(): void
    {
        static::assertNull(LiteralDomain::UnsignedInteger64->key(1.0));
        static::assertNull(LiteralDomain::Boolean->key(true));
        static::assertNull(LiteralDomain::None->key('a'));
        static::assertNull(LiteralDomain::Bytes->key(1));
        static::assertNull(LiteralDomain::DateTime->key('2024-01-01'));
        static::assertNull(LiteralDomain::Number->key('1'));
        static::assertNull(LiteralDomain::Number->key(2 ** 53 + 1));
        static::assertNull(LiteralDomain::Number->key(-2 ** 53 - 1));
        static::assertNull(LiteralDomain::Number->key((float) 2 ** 60));
    }
}
