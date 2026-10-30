<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Unit;

use Flow\Azure\SDK\BlobService\ListBlobs\OptionInclude;
use Flow\Azure\SDK\BlobService\ListBlobs\OptionShowOnly;
use Flow\Filesystem\Bridge\Azure\Options;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Local\Permissions;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;
use function sys_get_temp_dir;

final class OptionsTest extends TestCase
{
    public function test_normalize_round_trips_every_option(): void
    {
        $options = (new Options())
            ->withBlockSize(1024)
            ->withFileFastPath(false)
            ->withListBlobInclude(OptionInclude::METADATA, OptionInclude::SNAPSHOTS)
            ->withListBlobMaxResults(50)
            ->withListBlobShowOnly(OptionShowOnly::FILES)
            ->withTmpDir(path('azure-blob://tmp/'));
        $normalized = $options->normalize();

        static::assertNotNull($normalized);
        static::assertSame(['metadata', 'snapshots'], $normalized['list_blob_include']);
        static::assertSame('files', $normalized['list_blob_show_only']);
        static::assertEquals($options, Options::fromArray($normalized));
    }

    public function test_the_defaults_round_trip(): void
    {
        $options = new Options();
        $normalized = $options->normalize();

        static::assertNotNull($normalized);
        static::assertNull($normalized['list_blob_include']);
        static::assertEquals($options, Options::fromArray($normalized));
    }

    public function test_block_permissions_round_trip(): void
    {
        $options = (new Options())->withBlockFactory(
            new NativeLocalFileBlocksFactory(sys_get_temp_dir(), Permissions::private()),
        );
        $normalized = $options->normalize();

        static::assertNotNull($normalized);
        static::assertSame(['file' => 0o600, 'directory' => 0o700], $normalized['block_permissions']);
        static::assertEquals($options, Options::fromArray($normalized));
    }

    public function test_a_custom_block_factory_does_not_normalize(): void
    {
        static::assertNull(
            (new Options())
                ->withBlockFactory($this->createStub(BlockFactory::class))
                ->normalize(),
        );
    }

    public function test_from_array_refuses_a_missing_key(): void
    {
        $this->expectException(InvalidArgumentException::class);

        Options::fromArray(['block_size' => 1]);
    }
}
