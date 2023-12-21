<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Partition;
use PHPUnit\Framework\TestCase;

final class PartitionTest extends TestCase
{
    /**
     * @return array<array<string>>
     */
    public static function provider_forbidden_characters_values() : array
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
            [''],
        ];
    }

    public function test_creating_partitions_from_uri_with_partition_with_forbidden_character() : void
    {
        $partitions = Partition::fromUri('/dataset/country=U|S/something');

        $this->assertCount(0, $partitions);
    }

    public function test_creating_partitions_from_uri_with_partitions() : void
    {
        $partitions = Partition::fromUri('/dataset/country=US/age-range=20-45');

        $this->assertCount(2, $partitions);
        $this->assertEquals(
            [
                new Partition('country', 'US'),
                new Partition('age-range', '20-45'),
            ],
            $partitions->toArray()
        );
    }

    public function test_creating_partitions_from_uri_without_partitions() : void
    {
        $partitions = Partition::fromUri('/some/regular/uri/to/file.csv');

        $this->assertCount(0, $partitions);
    }

    /**
     * @dataProvider provider_forbidden_characters_values
     */
    public function test_forbidden_names(string $value) : void
    {
        $this->expectException(InvalidArgumentException::class);

        new Partition($value, 'value');
    }

    /**
     * @dataProvider provider_forbidden_characters_values
     */
    public function test_forbidden_values(string $value) : void
    {
        $this->expectException(InvalidArgumentException::class);

        new Partition('name', $value);
    }
}
