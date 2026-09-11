<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\Service\Examples;

use Flow\Website\Service\Examples\ExampleMeta;
use Flow\Website\Service\Examples\InvalidExampleMetaException;
use Flow\Website\Tests\Context\ExampleMetaContext;
use PHPUnit\Framework\TestCase;

final class ExampleMetaTest extends TestCase
{
    private ExampleMetaContext $meta;

    protected function setUp(): void
    {
        $this->meta = new ExampleMetaContext();
    }

    protected function tearDown(): void
    {
        $this->meta->clean();
    }

    public function test_a_directory_without_meta_falls_back_to_the_default(): void
    {
        $meta = ExampleMeta::fromDirectory($this->meta->directoryWithout());

        static::assertSame(99, $meta->priority);
        static::assertFalse($meta->hidden);
        static::assertNull($meta->skipReason);
    }

    public function test_priority_and_hidden_are_read(): void
    {
        $meta = $this->meta->parse("priority: 3\nhidden: true\n");

        static::assertSame(3, $meta->priority);
        static::assertTrue($meta->hidden);
    }

    public function test_a_skip_reason_is_read(): void
    {
        static::assertSame(
            'needs credentials',
            $this->meta->parse("priority: 3\nrun:\n  skip: \"needs credentials\"\n")->skipReason,
        );
    }

    public function test_meta_without_a_run_key_has_no_skip_reason(): void
    {
        static::assertNull($this->meta->parse("priority: 3\n")->skipReason);
    }

    public function test_a_priority_left_at_the_default_is_a_hard_failure(): void
    {
        $this->expectException(InvalidExampleMetaException::class);
        $this->expectExceptionMessageMatches('/"priority" must be an integer other than 99/');

        $this->meta->parse("priority: 99\n");
    }

    public function test_meta_without_a_priority_is_a_hard_failure(): void
    {
        $this->expectException(InvalidExampleMetaException::class);
        $this->expectExceptionMessageMatches('/"priority" must be an integer other than 99/');

        $this->meta->parse("hidden: false\n");
    }

    public function test_a_non_boolean_hidden_is_a_hard_failure(): void
    {
        $this->expectException(InvalidExampleMetaException::class);
        $this->expectExceptionMessageMatches('/"hidden" must be a boolean/');

        $this->meta->parse("priority: 3\nhidden: \"yes\"\n");
    }

    public function test_an_unknown_top_level_key_is_a_hard_failure(): void
    {
        $this->expectException(InvalidExampleMetaException::class);
        $this->expectExceptionMessageMatches('/unknown key "prioriy"/');

        $this->meta->parse("priority: 3\nprioriy: 3\n");
    }

    public function test_a_misspelled_run_key_is_a_hard_failure(): void
    {
        $this->expectException(InvalidExampleMetaException::class);
        $this->expectExceptionMessageMatches('/unknown key "run.sikp"/');

        $this->meta->parse("priority: 3\nrun:\n  sikp: \"a\"\n");
    }

    public function test_run_must_be_a_map(): void
    {
        $this->expectException(InvalidExampleMetaException::class);
        $this->expectExceptionMessageMatches('/"run" must be a map/');

        $this->meta->parse("priority: 3\nrun: nope\n");
    }

    public function test_an_empty_skip_reason_is_a_hard_failure(): void
    {
        $this->expectException(InvalidExampleMetaException::class);
        $this->expectExceptionMessageMatches('/"run.skip" must be a non-empty string/');

        $this->meta->parse("priority: 3\nrun:\n  skip: \"   \"\n");
    }

    public function test_the_root_must_be_a_map(): void
    {
        $this->expectException(InvalidExampleMetaException::class);
        $this->expectExceptionMessageMatches('/"<root>" must be a map/');

        $this->meta->parse("just a string\n");
    }
}
