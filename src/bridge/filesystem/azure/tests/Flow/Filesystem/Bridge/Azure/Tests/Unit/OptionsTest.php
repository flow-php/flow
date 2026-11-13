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
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;
use function sprintf;
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

    public function test_copy_part_size_defaults_to_256_mib(): void
    {
        static::assertSame(268435456, (new Options())->copyPartSize());
    }

    #[TestWith([0])]
    #[TestWith([(256 * 1024 * 1024) + 1])]
    public function test_copy_part_size_outside_1_byte_to_256_mib_is_refused(int $bytes): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf(
            'Copy part size must be between 1 byte and 256 MiB, got %d bytes',
            $bytes,
        ));

        (new Options())->withCopyPartSize($bytes);
    }

    public function test_copy_part_size_round_trips(): void
    {
        $options = (new Options())->withCopyPartSize(5 * 1024 * 1024);
        $normalized = $options->normalize();

        static::assertNotNull($normalized);
        static::assertSame(5 * 1024 * 1024, $normalized['copy_part_size']);
        static::assertEquals($options, Options::fromArray($normalized));
    }

    public function test_from_array_refuses_a_configuration_without_copy_part_size(): void
    {
        $normalized = (new Options())->normalize();
        static::assertNotNull($normalized);
        unset($normalized['copy_part_size']);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Azure options must hold block_location, block_permissions, block_size, copy_part_size, file_fast_path, list_blob_include, list_blob_max_results, list_blob_show_only and tmp_dir',
        );

        Options::fromArray($normalized);
    }
}
