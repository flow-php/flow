<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\cast;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Row;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use PHPUnit\Framework\TestCase;

final class CastTest extends TestCase
{
    public function cast_provider() : array
    {
        return [
            'int' => ['1', 'int', 1],
            'integer' => ['1', 'integer', 1],
            'float' => ['1', 'float', 1.0],
            'double' => ['1', 'double', 1.0],
            'real' => ['1', 'real', 1.0],
            'string' => [1, 'string', '1'],
            'bool' => ['1', 'bool', true],
            'boolean' => ['1', 'boolean', true],
            'array' => ['1', 'array', ['1']],
            'object' => ['1', 'object', (object) '1'],
            'null' => ['1', 'null', null],
            'json' => [[1], 'json', '[1]'],
            'json_pretty' => [[1], 'json_pretty', "[\n    1\n]"],
        ];
    }

    /**
     * @dataProvider cast_provider
     */
    public function test_cast(mixed $from, string $to, mixed $expected) : void
    {
        $this->assertEquals(
            $expected,
            ref('value')->cast($to)->eval(Row::create((new NativeEntryFactory())->create('value', $from)))
        );
        $this->assertEquals(
            $expected,
            cast(ref('value'), $to)->eval(Row::create((new NativeEntryFactory())->create('value', $from)))
        );
    }
}
