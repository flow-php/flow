<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Path;

use Flow\Filesystem\Path\Options;
use PHPUnit\Framework\TestCase;

final class OptionsTest extends TestCase
{
    public function test_get_option(): void
    {
        $options = new Options([
            'foo' => 'bar',
        ]);

        static::assertEquals('bar', $options->get('foo'));
        static::assertTrue($options->has('foo'));
        static::assertFalse($options->has('boo'));
        static::assertEquals(
            [
                'foo' => 'bar',
            ],
            $options->toArray(),
        );
    }

    public function test_set_option(): void
    {
        $options = new Options([
            'foo' => 'bar',
        ]);

        $options = $options->set('foo', 'baz');

        static::assertEquals('baz', $options->get('foo'));
    }

    public function test_set_option_when_empty(): void
    {
        $options = new Options([]);
        $options = $options->setWhenEmpty('foo', 'baz');
        $options = $options->setWhenEmpty('foo', 'bar');
        static::assertEquals('baz', $options->get('foo'));
    }
}
