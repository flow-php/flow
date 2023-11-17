<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class PregReplaceTest extends TestCase
{
    public function test_preg_replace() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('preg_replace', ref('key')->regexReplace(lit('/e/'), lit('es')))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'value', 'preg_replace' => 'values'],
            ],
            $memory->data
        );
    }

    public function test_preg_replace_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('preg_replace', ref('id')->regexReplace(lit('1'), lit(1)))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'preg_replace' => null],
            ],
            $memory->data
        );
    }

    public function test_preg_replace_on_non_string_value() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => '1'],
                    ]
                )
            )
            ->withEntry('preg_replace', ref('id')->regexReplace(lit(1), lit('1')))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => '1', 'preg_replace' => null],
            ],
            $memory->data
        );
    }
}
