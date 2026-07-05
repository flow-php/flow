<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\HTMLEntry;
use Flow\ETL\Row\Entry\XMLEntry;
use Flow\Filesystem\Partition;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\html_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\xml_entry;

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

    public function test_creating_partition_value_date_entry(): void
    {
        static::assertEquals('2023-01-01', Partition::valueFromRow(
            ref('date'),
            row(datetime_entry('date', '2023-01-01 00:00:00 UTC')),
        ));
    }

    public function test_creating_partition_value_datetime_entry(): void
    {
        static::assertEquals('2023-01-01', Partition::valueFromRow(
            ref('date'),
            row(datetime_entry('date', '2023-01-01 21:51:14 PST')),
        ));
    }

    #[RequiresPhp('>= 8.4')]
    public function test_creating_partition_value_from_html_entry(): void
    {
        $this->expectExceptionMessage(HTMLEntry::class . ' can\'t be used as a partition');

        Partition::valueFromRow(
            ref('html'),
            row(html_entry('html', '<!DOCTYPE html><html><head></head><body></body></html>')),
        );
    }

    public function test_creating_partition_value_from_xml_entry(): void
    {
        $this->expectExceptionMessage(XMLEntry::class . ' can\'t be used as a partition');

        Partition::valueFromRow(ref('xml'), row(xml_entry('xml', '<xml></xml>')));
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
