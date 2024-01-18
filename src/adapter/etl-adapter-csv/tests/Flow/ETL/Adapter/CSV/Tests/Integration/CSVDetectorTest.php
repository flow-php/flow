<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\CSVDetector;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CSVDetectorTest extends TestCase
{
    public static function enclosure_provider() : \Generator
    {
        yield ['double_quote' => '"'];
        yield ['single_quote' => "'"];
    }

    public static function separator_provider() : \Generator
    {
        yield ['comma' => ','];
        yield ['tab' => "\t"];
        yield ['semicolon' => ';'];
        yield ['pipe' => '|'];
        yield ['space' => ' '];
        yield ['underscore' => '_'];
        yield ['dash' => '-'];
        yield ['double_dot' => ':'];
        yield ['tilde' => '~'];
        yield ['at' => '@'];
        yield ['hash' => '#'];
        yield ['dollar' => '$'];
        yield ['percent' => '%'];
        yield ['caret' => '^'];
        yield ['ampersand' => '&'];
        yield ['asterisk' => '*'];
        yield ['left_parenthesis' => '('];
        yield ['right_parenthesis' => ')'];
        yield ['plus' => '+'];
        yield ['equal' => '='];
        yield ['question_mark' => '?'];
        yield ['exclamation_mark' => '!'];
        yield ['backslash' => '\\'];
        yield ['slash' => '/'];
        yield ['dot' => '.'];
        yield ['greater_than' => '>'];
        yield ['less_than' => '<'];
    }

    #[DataProvider('enclosure_provider')]
    public function test_detecting_enclosures(string $enclosure) : void
    {
        $detector = new CSVDetector($this->createResource(',', $enclosure));

        $this->assertSame($enclosure, $detector->detect()->enclosure);
    }

    #[DataProvider('separator_provider')]
    public function test_detecting_separators(string $separator) : void
    {
        $detector = new CSVDetector($this->createResource($separator));

        $this->assertSame($separator, $detector->detect()->separator);
    }

    /**
     * @return resource
     */
    private function createResource(string $separator = ',', string $enclosure = '"')
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
            \fputcsv($resource, $line, $separator, $enclosure);
        }

        \rewind($resource);

        return $resource;
    }
}
