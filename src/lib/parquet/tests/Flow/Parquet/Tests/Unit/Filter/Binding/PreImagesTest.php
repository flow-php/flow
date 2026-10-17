<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding;

use Flow\Parquet\Filter\Binding\PreImage;
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
use Flow\Parquet\Filter\Binding\PreImages;
use Flow\Parquet\Filter\Operator;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\Tests\Context\PhysicalIntervalEdges;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use PHPUnit\Framework\TestCase;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class PreImagesTest extends TestCase
{
    public function test_a_column_whose_decoded_values_have_no_literal_kind_is_unbound(): void
    {
        static::assertNull(PhysicalIntervalEdges::intervals(FlatColumnMother::int96(), Operator::Equal, 1));
        static::assertNull(PhysicalIntervalEdges::intervals(FlatColumn::time('t'), Operator::Equal, 1));
        static::assertNull(PhysicalIntervalEdges::intervals(FlatColumnMother::float16(), Operator::Equal, "\x00\x3C"));
        static::assertNull(PhysicalIntervalEdges::intervals(FlatColumn::date('d'), Operator::Equal, 19_723));
        static::assertNull(PhysicalIntervalEdges::intervals(FlatColumn::dateTime('ts'), Operator::Equal, 0));
    }

    public function test_each_column_gets_the_pre_image_of_its_decoded_values(): void
    {
        $preImage = static fn(FlatColumn $column): ?PreImage => PreImages::forColumn(
            $column,
            DataConverter::initialize(new Options())->resolveConverter($column),
        );

        static::assertInstanceOf(BooleanPreImage::class, $preImage(FlatColumn::boolean('b')));
        static::assertEquals(new IntegerPreImage(PHP_INT_MIN, PHP_INT_MAX), $preImage(FlatColumn::int64('i')));
        static::assertEquals(new IntegerPreImage(-2_147_483_648, 2_147_483_647), $preImage(FlatColumn::int32('i')));
        static::assertEquals(new IntegerPreImage(0, 255), $preImage(FlatColumnMother::uint8()));
        static::assertEquals(new IntegerPreImage(0, 4_294_967_295), $preImage(FlatColumnMother::uint32()));
        static::assertInstanceOf(UnsignedInt64PreImage::class, $preImage(FlatColumnMother::uint64()));
        static::assertInstanceOf(DoublePreImage::class, $preImage(FlatColumn::double('d')));
        static::assertInstanceOf(Float32PreImage::class, $preImage(FlatColumn::float('f')));
        static::assertInstanceOf(DatePreImage::class, $preImage(FlatColumn::date('d')));
        static::assertEquals(new TimestampPreImage(TimeUnit::MICROSECONDS), $preImage(FlatColumn::dateTime('t')));
        static::assertInstanceOf(DecimalPreImage::class, $preImage(FlatColumn::decimal('d', 10, 2)));
        static::assertInstanceOf(UuidPreImage::class, $preImage(FlatColumn::uuid('u')));
        static::assertInstanceOf(BytesPreImage::class, $preImage(FlatColumn::string('s')));
        static::assertInstanceOf(BytesPreImage::class, $preImage(FlatColumn::enum('e')));
        static::assertNull($preImage(FlatColumn::json('j')));
        static::assertNull($preImage(FlatColumnMother::interval()));
        static::assertNull($preImage(FlatColumnMother::byteArrayDecimal()));
    }
}
