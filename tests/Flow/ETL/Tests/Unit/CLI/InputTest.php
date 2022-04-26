<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\CLI;

use Flow\ETL\CLI\Input;
use PHPUnit\Framework\TestCase;

final class InputTest extends TestCase
{
    public function test_default_option_value() : void
    {
        $input = new Input(['bin/worker']);

        $this->assertSame(
            'default',
            $input->optionValue('host', 'default')
        );
    }

    public function test_option_value() : void
    {
        $input = new Input(['bin/worker', '--host=127.0.0.1']);

        $this->assertSame(
            '127.0.0.1',
            $input->optionValue('host', 'default')
        );
    }

    public function test_option_value_with_invalid_option_format() : void
    {
        $input = new Input(['bin/worker', 'host=127.0.0.1']);

        $this->assertSame(
            'default',
            $input->optionValue('host', 'default')
        );
    }
}
