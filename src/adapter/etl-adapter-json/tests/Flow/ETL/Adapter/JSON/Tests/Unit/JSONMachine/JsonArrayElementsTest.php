<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonArrayElements;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use JsonMachine\Items;
use JsonMachine\JsonDecoder\ExtJsonDecoder;
use PHPUnit\Framework\Attributes\DataProvider;
use Throwable;

use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function str_repeat;

final class JsonArrayElementsTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{string}>
     */
    public static function documents(): Generator
    {
        yield 'empty array' => ['[]'];
        yield 'whitespace around everything' => ["  [ {\"a\":1} ,\n {\"b\":[1,{\"c\":\"]}\"}]} ]  "];
        yield 'byte order mark' => ["\u{FEFF}[{\"a\":1},{\"b\":2}]"];
        yield 'escaped quotes and backslashes' => ['[{"s":"q\\"}\\\\"},{"t":"\\\\"}]'];
        yield 'brackets inside strings' => ['[{"s":"[{]}"},["]",{"x":"}"}]]'];
        yield 'nested arrays as elements' => ['[[1,2],[3],[]]'];
        yield 'unicode escapes' => ['[{"s":"\\u00e9\\u0105"}]'];
        yield 'an element larger than any refill' => ['[{"s":"' . str_repeat('x', 100) . '"},{"n":1}]'];
        yield 'trailing text after the array' => ['[{"a":1}] trailing'];
        yield 'a top-level object' => ['{"x":{"a":1},"y":{"b":2}}'];
        yield 'scalar elements only' => ['[1,"a",null,true]'];
        yield 'a scalar after objects' => ['[{"a":1},2,{"b":3}]'];
        yield 'a trailing comma' => ['[{"a":1},]'];
        yield 'a missing comma' => ['[{"a":1} {"b":2}]'];
        yield 'an unquoted key' => ['[{"a":1},{b:2}]'];
        yield 'an unterminated array' => ['[{"a":1}'];
        yield 'an unterminated element' => ['[{"a":1},{"b":'];
        yield 'whitespace only' => ['     '];
    }

    #[DataProvider('documents')]
    public function test_yields_or_refuses_exactly_what_json_machine_does(string $document): void
    {
        $options = ['decoder' => new ExtJsonDecoder(true)];

        try {
            $expected = iterator_to_array(Items::fromString($document, $options));
        } catch (Throwable $exception) {
            $expected = $exception::class;
        }

        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://document.json'))->append($document)->close();
        $stream = $filesystem->readFrom(path('memory://document.json'));

        $fallback = static function (int $skip) use ($document, $options): Generator {
            foreach (Items::fromString($document, $options) as $key => $item) {
                if ($skip > 0) {
                    $skip--;

                    continue;
                }

                yield $key => $item;
            }
        };

        try {
            $actual = iterator_to_array((new JsonArrayElements(4))->of($stream, $stream->read(5, 0), $fallback));
        } catch (Throwable $exception) {
            $actual = $exception::class;
        }

        static::assertSame($expected, $actual);
    }
}
