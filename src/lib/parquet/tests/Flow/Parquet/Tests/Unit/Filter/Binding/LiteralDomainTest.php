<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\LiteralDomain;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use PHPUnit\Framework\TestCase;

final class LiteralDomainTest extends TestCase
{
    public function test_each_column_kind_maps_to_the_domain_of_its_decoded_values(): void
    {
        $domain = static fn(FlatColumn $column): LiteralDomain => LiteralDomain::forColumn(
            $column,
            DataConverter::initialize(new Options())->resolveConverter($column),
        );

        static::assertSame(LiteralDomain::Boolean, $domain(FlatColumn::boolean('b')));
        static::assertSame(LiteralDomain::Number, $domain(FlatColumn::int32('i')));
        static::assertSame(LiteralDomain::Number, $domain(FlatColumn::double('d')));
        static::assertSame(LiteralDomain::Number, $domain(FlatColumn::decimal('m', 10, 2)));
        static::assertSame(LiteralDomain::Number, $domain(FlatColumnMother::uint32()));
        static::assertSame(LiteralDomain::UnsignedInteger64, $domain(FlatColumnMother::uint64()));
        static::assertSame(LiteralDomain::Bytes, $domain(FlatColumn::string('s')));
        static::assertSame(LiteralDomain::Bytes, $domain(FlatColumn::json('j')));
        static::assertSame(LiteralDomain::Uuid, $domain(FlatColumn::uuid('u')));
        static::assertSame(LiteralDomain::DateTime, $domain(FlatColumn::date('day')));
        static::assertSame(LiteralDomain::DateTime, $domain(FlatColumn::dateTime('ts')));
        static::assertSame(LiteralDomain::None, $domain(FlatColumn::time('t')));
        static::assertSame(LiteralDomain::None, $domain(FlatColumnMother::float16()));
        static::assertSame(LiteralDomain::None, $domain(FlatColumnMother::interval()));
        static::assertSame(LiteralDomain::None, LiteralDomain::forColumn(FlatColumnMother::int96(), null));
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
        static::assertTrue(LiteralDomain::Bytes->holdsEach(['a', 'b']));
        static::assertFalse(LiteralDomain::Bytes->holdsEach(['a', 1]));
        static::assertTrue(LiteralDomain::Uuid->holdsPrefix());
        static::assertSame(
            '00000000-0000-4000-8000-0000000000aa',
            LiteralDomain::Uuid->normalise('00000000-0000-4000-8000-0000000000AA'),
        );
        static::assertSame('ABC', LiteralDomain::Bytes->normalise('ABC'));
        static::assertFalse(LiteralDomain::Number->holdsPrefix());
    }
}
