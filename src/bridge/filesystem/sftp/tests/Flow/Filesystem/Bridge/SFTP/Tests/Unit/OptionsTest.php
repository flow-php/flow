<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\SFTP\Options;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Stream\BlockFactory;

use function Flow\Filesystem\Bridge\SFTP\DSL\sftp_filesystem_options;
use function Flow\Filesystem\DSL\path;

final class OptionsTest extends FlowTestCase
{
    public function test_block_size_below_one_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Block size must be greater than 0, given: 0');

        sftp_filesystem_options()->withBlockSize(0);
    }

    public function test_read_chunk_size_below_one_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Read chunk size must be greater than 0, given: -1');

        sftp_filesystem_options()->withReadChunkSize(-1);
    }

    public function test_normalize_round_trips(): void
    {
        $options = sftp_filesystem_options()
            ->withBlockSize(1024)
            ->withReadChunkSize(512)
            ->withTmpDir(path('sftp://tmp/'));

        $normalized = $options->normalize();

        static::assertNotNull($normalized);
        static::assertSame(1024, $normalized['block_size']);
        static::assertSame(512, $normalized['read_chunk_size']);
        static::assertEquals($options, Options::fromArray($normalized));
    }

    public function test_a_custom_block_factory_does_not_normalize(): void
    {
        static::assertNull(
            sftp_filesystem_options()->withBlockFactory($this->createStub(BlockFactory::class))->normalize(),
        );
    }

    public function test_from_array_refuses_a_missing_key(): void
    {
        $this->expectException(InvalidArgumentException::class);

        Options::fromArray(['block_size' => 1]);
    }
}
