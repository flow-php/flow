<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\RustIterator;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function preg_quote;
use function str_repeat;

#[RequiresPhpExtension('flow_php')]
final class RustJsonRefusalTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{JsonFormat, string, string}>
     */
    public static function malformed(): Generator
    {
        $deep = str_repeat('[', 512) . str_repeat(']', 512);

        yield 'two records on a line' => [JsonFormat::Lines, '{"id":1}{"id":9}', 'at line 1: '];
        yield 'a NUL line' => [JsonFormat::Lines, "{\"id\":1}\n\0\n", 'at line 2: '];
        yield 'a BOM on the second line' => [JsonFormat::Lines, "{\"id\":1}\n\u{FEFF}{\"id\":2}\n", 'at line 2: '];
        yield 'a vertical tab between tokens' => [JsonFormat::Document, "[{\"id\":1}\v,{\"id\":2}]", 'at element 0: '];
        yield 'a trailing comma' => [JsonFormat::Document, '[{"id":1},]', 'at element 1: '];
        yield 'a missing comma' => [JsonFormat::Document, '[{"id":1} {"id":2}]', 'at element 0: '];
        yield 'content after the array' => [JsonFormat::Document, '[{"id":1}] x', 'at element 1: '];
        yield 'a truncated document' => [JsonFormat::Document, '[{"id":1}', 'at element 0: '];
        yield 'invalid UTF-8 in a skipped member of an element' => [
            JsonFormat::Document,
            "[{\"id\":1,\"skipped\":\"\xFF\"}]",
            'at element 0: ',
        ];
        yield 'invalid UTF-8 in a skipped member of a line' => [
            JsonFormat::Lines,
            "{\"id\":1,\"skipped\":\"\xFF\"}",
            'at line 1: ',
        ];
        yield 'a lone surrogate escape in a skipped member of an element' => [
            JsonFormat::Document,
            '[{"id":1,"skipped":"\\ud800"}]',
            'at element 0: Single unpaired UTF-16 surrogate in unicode escape',
        ];
        yield 'a high surrogate escape without its low one in a line' => [
            JsonFormat::Lines,
            '{"id":1,"skipped":"\\ud83d\\u0041"}',
            'at line 1: Single unpaired UTF-16 surrogate in unicode escape',
        ];
        yield 'a raw control character in a string' => [JsonFormat::Lines, "{\"id\":\"\x01\"}", 'at line 1: '];
        yield 'a leading zero' => [JsonFormat::Lines, '{"id":01}', 'at line 1: '];
        yield 'NaN' => [JsonFormat::Lines, '{"id":NaN}', 'at line 1: '];
        yield 'True' => [JsonFormat::Lines, '{"id":True}', 'at line 1: '];
        yield 'an element nested as deep as json_decode refuses' => [
            JsonFormat::Document,
            '[{"id":' . $deep . '}]',
            'at element 0: ',
        ];
        yield 'a line nested as deep as json_decode refuses' => [
            JsonFormat::Lines,
            '{"id":' . $deep . '}',
            'at line 1: ',
        ];
    }

    #[DataProvider('malformed')]
    public function test_malformed_json_is_refused_naming_the_file_and_the_record(
        JsonFormat $format,
        string $content,
        string $record,
    ): void {
        $open = JsonFixtureContext::open($format, new SourceFile(path('memory://a.json')), MemoryFiles::with([
            'memory://a.json' => $content,
        ]));
        static::assertInstanceOf(RustIterator::class, $open->batches(schema(int_schema('id')), 2, new PhpBackend()));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches(
            '/^' . preg_quote('Malformed JSON in "memory://a.json" ' . $record, '/') . '/',
        );

        try {
            iterator_to_array($open->batches(schema(int_schema('id', nullable: true)), 2, new PhpBackend()), false);
        } finally {
            $open->close();
        }
    }

    public function test_a_scalar_line_is_refused_as_a_scalar_record(): void
    {
        $open = JsonFixtureContext::open(
            JsonFormat::Lines,
            new SourceFile(path('memory://a.jsonl')),
            MemoryFiles::with([
                'memory://a.jsonl' => "{\"id\":1}\n5\n",
            ]),
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('A JSON record must be an object or an array, int given in "memory://a.jsonl".');

        try {
            iterator_to_array($open->batches(schema(int_schema('id')), 2, new PhpBackend()), false);
        } finally {
            $open->close();
        }
    }
}
