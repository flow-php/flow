<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\CSVDetector;
use Flow\ETL\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;

final class CSVDetectorTest extends TestCase
{
    public function test_detecting_comma_delimiter() : void
    {
        $detector = new CSVDetector($this->createResource(','));

        $this->assertSame(',', $detector->separator());
    }

    public function test_detecting_comma_with_custom_enclosure() : void
    {
        $detector = new CSVDetector($this->createResource(',', "'"));

        $this->assertSame(',', $detector->separator());
    }

    public function test_detecting_dash() : void
    {
        $detector = new CSVDetector($this->createResource('-'));

        $this->assertSame('-', $detector->separator());
    }

    public function test_detecting_double_dot() : void
    {
        $detector = new CSVDetector($this->createResource(':'));

        $this->assertSame(':', $detector->separator());
    }

    public function test_detecting_no_delimiter() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot detect delimiter');

        $detector = new CSVDetector($this->createResource('{'));
        $detector->separator();
    }

    public function test_detecting_pipe() : void
    {
        $detector = new CSVDetector($this->createResource('|'));

        $this->assertSame('|', $detector->separator());
    }

    public function test_detecting_semicolon() : void
    {
        $detector = new CSVDetector($this->createResource(';'));

        $this->assertSame(';', $detector->separator());
    }

    public function test_detecting_space() : void
    {
        $detector = new CSVDetector($this->createResource(' '));

        $this->assertSame(' ', $detector->separator());
    }

    public function test_detecting_tab_delimiter() : void
    {
        $detector = new CSVDetector($this->createResource("\t"));

        $this->assertSame("\t", $detector->separator());
    }

    public function test_detecting_underscore() : void
    {
        $detector = new CSVDetector($this->createResource('_'));

        $this->assertSame('_', $detector->separator());
    }

    /**
     * @return resource
     */
    private function createResource(string $separator, string $enclosure = '"')
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
