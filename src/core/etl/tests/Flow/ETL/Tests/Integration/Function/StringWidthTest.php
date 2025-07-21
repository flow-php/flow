<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, greatest, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class StringWidthTest extends FlowTestCase
{
    public function test_width() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello'],
                        ['text' => '中文'],
                        ['text' => 'ひらがな'],
                        ['text' => '한글'],
                        ['text' => '🚀'],
                        ['text' => ''],
                        ['text' => null],
                        ['text' => 'a'],
                        ['text' => 'hello中文'],
                        ['text' => '！'],
                    ]
                )
            )
            ->withEntry('width', ref('text')->stringWidth())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello', 'width' => 5],
                ['text' => '中文', 'width' => 4],
                ['text' => 'ひらがな', 'width' => 8],
                ['text' => '한글', 'width' => 4],
                ['text' => '🚀', 'width' => 2],
                ['text' => '', 'width' => 0],
                ['text' => null, 'width' => null],
                ['text' => 'a', 'width' => 1],
                ['text' => 'hello中文', 'width' => 9],
                ['text' => '！', 'width' => 2],
            ],
            $memory->dump()
        );
    }

    public function test_width_for_console_formatting() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['name' => 'John', 'name_cjk' => '张三'],
                        ['name' => 'Jane Smith', 'name_cjk' => '李明华'],
                        ['name' => 'Bob', 'name_cjk' => '王五'],
                    ]
                )
            )
            ->withEntry('name_width', ref('name')->stringWidth())
            ->withEntry('name_cjk_width', ref('name_cjk')->stringWidth())
            ->withEntry('max_width', greatest(ref('name_width'), ref('name_cjk_width')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['name' => 'John', 'name_cjk' => '张三', 'name_width' => 4, 'name_cjk_width' => 4, 'max_width' => 4],
                ['name' => 'Jane Smith', 'name_cjk' => '李明华', 'name_width' => 10, 'name_cjk_width' => 6, 'max_width' => 10],
                ['name' => 'Bob', 'name_cjk' => '王五', 'name_width' => 3, 'name_cjk_width' => 4, 'max_width' => 4],
            ],
            $memory->dump()
        );
    }
}
