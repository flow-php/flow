<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration\IO\Extractor;

use Flow\ETL\Adapter\CSV\IO\Extractor\CSVDetector;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\MemorySourceStream;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use RuntimeException;

use function fclose;
use function fopen;
use function fputcsv;
use function implode;
use function str_repeat;
use function stream_get_contents;

final class CSVDetectorTest extends FlowTestCase
{
    public static function enclosure_provider(): Generator
    {
        yield ['enclosure' => '"'];
        yield ['enclosure' => "'"];
    }

    public static function multibyte_separator_provider(): Generator
    {
        yield ['separator' => ';'];
        yield ['separator' => ','];
        yield ['separator' => "\t"];
    }

    public static function separator_provider(): Generator
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
    public function test_detecting_enclosures(string $enclosure): void
    {
        $detector = new CSVDetector($this->createResource(',', $enclosure));

        static::assertSame($enclosure, $detector->detect()->enclosure);
    }

    #[DataProvider('multibyte_separator_provider')]
    public function test_detecting_a_separator_in_multibyte_text(string $separator): void
    {
        $line = implode($separator, ['zażółć', 'gęślą', 'jaźń']) . "\n";
        $detector = new CSVDetector(new MemorySourceStream(str_repeat($line, 5)));

        static::assertSame($separator, $detector->detect()->separator);
    }

    #[DataProvider('separator_provider')]
    public function test_detecting_separators(string $separator): void
    {
        $detector = new CSVDetector($this->createResource($separator));

        static::assertSame($separator, $detector->detect()->separator);
    }

    /**
     * @return SourceStream
     */
    private function createResource(string $separator = ',', string $enclosure = '"'): SourceStream
    {
        /** @var array<int, array<int, string>> $data */
        $data = [
            ['id', 'name',     'email'],
            ['1',  'John Doe', 'john@example.com'],
            ['2',  'Jane Doe', 'jane@example.com'],
            ['3',  'Mark',     'mark@example.com'],
            ['4',  'Kate',     'kate@example.com'],
            ['5',  'Peter',    'peter@example.com'],
            ['6',  'Paul',     'paul@example.com'],
            ['7',  'Mary',     'mary@example.com'],
            ['8',  'Anna',     'anna@example.com'],
            ['9',  'Robert',   'rober@example.com'],
            ['10', 'Lucy',     'lucy@example.com'],
            ['11', 'Ro\'bert', 'rob_ert@example.com'],
        ];

        $resource = fopen('php://memory', 'rb+');

        if ($resource === false) {
            throw new RuntimeException('Failed to open memory stream');
        }

        foreach ($data as $line) {
            fputcsv($resource, $line, $separator, $enclosure, '\\');
        }

        $csv = stream_get_contents($resource, offset: 0);
        fclose($resource);

        if ($csv === false) {
            throw new RuntimeException('Failed to read stream contents');
        }

        if ($csv === '') {
            throw new RuntimeException('Stream content is empty');
        }

        return new MemorySourceStream($csv);
    }
}
