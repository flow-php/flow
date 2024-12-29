<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\CSVDetector;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\MemorySourceStream;
use PHPUnit\Framework\Attributes\DataProvider;

final class CSVDetectorTest extends FlowTestCase
{
    public static function enclosure_provider() : \Generator
    {
        yield ['enclosure' => '"'];
        yield ['enclosure' => "'"];
    }

    public static function separator_provider() : \Generator
    {
        yield ['separator' => ','];
        yield ['separator' => "\t"];
        yield ['separator' => ';'];
        yield ['separator' => '|'];
        yield ['separator' => ' '];
        yield ['separator' => '_'];
        yield ['separator' => '-'];
        yield ['separator' => ':'];
        yield ['separator' => '~'];
        yield ['separator' => '@'];
        yield ['separator' => '#'];
        yield ['separator' => '$'];
        yield ['separator' => '%'];
        yield ['separator' => '^'];
        yield ['separator' => '&'];
        yield ['separator' => '*'];
        yield ['separator' => '('];
        yield ['separator' => ')'];
        yield ['separator' => '+'];
        yield ['separator' => '='];
        yield ['separator' => '?'];
        yield ['separator' => '!'];
        yield ['separator' => '\\'];
        yield ['separator' => '/'];
        yield ['separator' => '.'];
        yield ['separator' => '>'];
        yield ['separator' => '<'];
    }

    #[DataProvider('enclosure_provider')]
    public function test_detecting_enclosures(string $enclosure) : void
    {
        $detector = new CSVDetector($this->createResource(',', $enclosure));

        self::assertSame($enclosure, $detector->detect()->enclosure);
    }

    #[DataProvider('separator_provider')]
    public function test_detecting_separators(string $separator) : void
    {
        $detector = new CSVDetector($this->createResource($separator));

        self::assertSame($separator, $detector->detect()->separator);
    }

    private function createResource(string $separator = ',', string $enclosure = '"') : SourceStream
    {
        $data = [
            ['id', 'name', 'email'],
            ['1', 'John Doe', 'john@example.com'],
            ['2', 'Jane Doe', 'jane@example.com'],
            ['3', 'Mark', 'mark@example.com'],
            ['4', 'Kate', 'kate@example.com'],
            ['5', 'Peter', 'peter@example.com'],
            ['6', 'Paul', 'paul@example.com'],
            ['7', 'Mary', 'mary@example.com'],
            ['8', 'Anna', 'anna@example.com'],
            ['9', 'Robert', 'rober@example.com'],
            ['10', 'Lucy', 'lucy@example.com'],
            ['11', 'Ro\'bert', 'rob_ert@example.com'],
        ];

        $resource = \fopen('php://memory', 'rb+');

        foreach ($data as $line) {
            \fputcsv($resource, $line, $separator, $enclosure, '\\');
        }

        $csv = \stream_get_contents($resource, offset: 0);
        \fclose($resource);

        return new MemorySourceStream($csv);
    }
}
