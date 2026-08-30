<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use DateTimeImmutable;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Partition;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_xml;

final class PartitionTest extends TestCase
{
    /**
     * @return array<array<string>>
     */
    public static function provider_forbidden_characters_values(): array
    {
        return [
            ['nam|e'],
            ['nam/e'],
            ['nam\e'],
            ['nam:e'],
            ['nam>e'],
            ['nam<e'],
            ['nam*e'],
            ['nam?e'],
            ['nam{e'],
            ['nam}e'],
            [''],
        ];
    }

    public function test_creating_partition_value_from_date(): void
    {
        static::assertEquals('2023-01-01', Partition::fromValue(
            'date',
            type_datetime(),
            new DateTimeImmutable('2023-01-01 00:00:00 UTC'),
        ));
    }

    public function test_creating_partition_value_from_datetime(): void
    {
        static::assertEquals('2023-01-01', Partition::fromValue(
            'date',
            type_datetime(),
            new DateTimeImmutable('2023-01-01 21:51:14 PST'),
        ));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_creating_partition_value_from_html(): void
    {
        $this->expectExceptionMessage('Column "html" of type html can\'t be used as a partition');

        Partition::fromValue('html', type_html(), '<!DOCTYPE html><html><head></head><body></body></html>');
    }

    public function test_creating_partition_value_from_xml(): void
    {
        $this->expectExceptionMessage('Column "xml" of type xml can\'t be used as a partition');

        Partition::fromValue('xml', type_xml(), '<xml></xml>');
    }

    public function test_creating_partitions_from_uri_with_partition_with_forbidden_character(): void
    {
        $partitions = Partition::fromUri('/dataset/country=U|S/something');

        static::assertCount(0, $partitions);
    }

    public function test_creating_partitions_from_uri_with_partitions(): void
    {
        $partitions = Partition::fromUri('/dataset/country=US/age-range=20-45');

        static::assertCount(2, $partitions);
        static::assertEquals(
            [
                new Partition('country', 'US'),
                new Partition('age-range', '20-45'),
            ],
            $partitions->toArray(),
        );
    }

    public function test_creating_partitions_from_uri_without_partitions(): void
    {
        $partitions = Partition::fromUri('/some/regular/uri/to/file.csv');

        static::assertCount(0, $partitions);
    }

    #[DataProvider('provider_forbidden_characters_values')]
    public function test_forbidden_names(string $value): void
    {
        $this->expectException(InvalidArgumentException::class);

        new Partition($value, 'value');
    }

    #[DataProvider('provider_forbidden_characters_values')]
    public function test_forbidden_values(string $value): void
    {
        $this->expectException(InvalidArgumentException::class);

        new Partition('name', $value);
    }
}
