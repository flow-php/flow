<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Testing;

use Closure;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Testing\FilesystemOperation;
use Flow\Filesystem\Testing\InterceptedFilesystem;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\intercepted_filesystem;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class InterceptedFilesystemTest extends TestCase
{
    /**
     * @return Generator<string, array{FilesystemOperation, string, Closure(Filesystem): mixed}>
     */
    public static function operations(): Generator
    {
        yield 'appendTo' => [
            FilesystemOperation::AppendTo,
            'memory://a.txt',
            static fn(Filesystem $fs): mixed => $fs->appendTo(path('memory://a.txt')),
        ];
        yield 'create' => [
            FilesystemOperation::Create,
            'memory://new.txt',
            static fn(Filesystem $fs): mixed => $fs->create(path('memory://new.txt')),
        ];
        yield 'list' => [
            FilesystemOperation::List,
            'memory://*.txt',
            static fn(Filesystem $fs): mixed => $fs->list(path('memory://*.txt')),
        ];
        yield 'mv' => [
            FilesystemOperation::Mv,
            'memory://a.txt',
            static fn(Filesystem $fs): mixed => $fs->mv(path('memory://a.txt'), path('memory://b.txt')),
        ];
        yield 'readFrom' => [
            FilesystemOperation::ReadFrom,
            'memory://a.txt',
            static fn(Filesystem $fs): mixed => $fs->readFrom(path('memory://a.txt')),
        ];
        yield 'rm' => [
            FilesystemOperation::Rm,
            'memory://a.txt',
            static fn(Filesystem $fs): mixed => $fs->rm(path('memory://a.txt')),
        ];
        yield 'status' => [
            FilesystemOperation::Status,
            'memory://a.txt',
            static fn(Filesystem $fs): mixed => $fs->status(path('memory://a.txt')),
        ];
        yield 'writeTo' => [
            FilesystemOperation::WriteTo,
            'memory://a.txt',
            static fn(Filesystem $fs): mixed => $fs->writeTo(path('memory://a.txt')),
        ];
    }

    public function test_cannot_be_rebuilt_from_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'InterceptedFilesystem holds a test hook and cannot be rebuilt from a configuration',
        );

        InterceptedFilesystem::fromArray([]);
    }

    /**
     * @param Closure(Filesystem): mixed $call
     */
    #[DataProvider('operations')]
    public function test_hook_receives_the_operation_and_the_path(
        FilesystemOperation $operation,
        string $uri,
        Closure $call,
    ): void {
        $inner = memory_filesystem();
        $inner->writeTo(path('memory://a.txt'))->append('a')->close();
        $calls = [];

        $call(intercepted_filesystem($inner)->onCall(static function (FilesystemOperation $called, Path $path) use (
            &$calls,
        ): void {
            $calls[] = [$called, $path->uri()];
        }));

        static::assertSame([[$operation, $uri]], $calls);
    }

    public function test_hook_that_throws_stops_the_delegation(): void
    {
        $inner = memory_filesystem();
        $inner->writeTo(path('memory://a.txt'))->append('a')->close();
        $exception = new RuntimeException('S3 down');
        $filesystem = intercepted_filesystem($inner)->onCall(static function (FilesystemOperation $operation) use (
            $exception,
        ): void {
            if ($operation === FilesystemOperation::Rm) {
                throw $exception;
            }
        });

        try {
            $filesystem->rm(path('memory://a.txt'));
            static::fail('rm() was expected to throw');
        } catch (RuntimeException $thrown) {
            static::assertSame($exception, $thrown);
        }

        static::assertNotNull($inner->status(path('memory://a.txt')));
    }

    public function test_later_on_call_replaces_the_earlier_hook(): void
    {
        $calls = [];
        $filesystem = intercepted_filesystem(memory_filesystem())
            ->onCall(static function (): void {
                throw new RuntimeException('earlier hook');
            })
            ->onCall(static function (FilesystemOperation $operation) use (&$calls): void {
                $calls[] = $operation;
            });

        static::assertNull($filesystem->status(path('memory://a.txt')));
        static::assertSame([FilesystemOperation::Status], $calls);
    }

    public function test_list_calls_the_hook_at_the_call_not_at_iteration(): void
    {
        $calls = [];
        $filesystem = intercepted_filesystem(
            memory_filesystem(),
        )->onCall(static function (FilesystemOperation $operation) use (&$calls): void {
            $calls[] = $operation;
        });

        $filesystem->list(path('memory://*.txt'));

        static::assertSame([FilesystemOperation::List], $calls);
    }

    public function test_mount_supports_and_system_tmp_dir_delegate(): void
    {
        $inner = native_local_filesystem();
        $filesystem = intercepted_filesystem($inner);

        static::assertSame($inner->mount(), $filesystem->mount());
        static::assertTrue($filesystem->supports(path('file:///tmp/a.txt')));
        static::assertFalse($filesystem->supports(path('memory://a.txt')));
        static::assertEquals($inner->getSystemTmpDir(), $filesystem->getSystemTmpDir());
    }

    public function test_normalize_returns_null(): void
    {
        static::assertNull(intercepted_filesystem(memory_filesystem())->normalize());
    }

    public function test_passes_every_operation_through_when_no_hook_is_set(): void
    {
        $inner = memory_filesystem();
        $filesystem = intercepted_filesystem($inner);

        $filesystem->writeTo(path('memory://a.txt'))->append('a')->close();
        $filesystem->appendTo(path('memory://a.txt'))->append('b')->close();
        $filesystem->create(path('memory://c.txt'))->append('c')->close();

        static::assertSame('ab', $filesystem->readFrom(path('memory://a.txt'))->content());
        static::assertSame('c', $inner->readFrom(path('memory://c.txt'))->content());
        static::assertEquals($inner->status(path('memory://a.txt')), $filesystem->status(path('memory://a.txt')));
        static::assertEquals(
            iterator_to_array($inner->list(path('memory://*.txt')), false),
            iterator_to_array($filesystem->list(path('memory://*.txt')), false),
        );

        static::assertTrue($filesystem->mv(path('memory://c.txt'), path('memory://d.txt')));
        static::assertNull($inner->status(path('memory://c.txt')));
        static::assertSame('c', $inner->readFrom(path('memory://d.txt'))->content());

        static::assertTrue($filesystem->rm(path('memory://a.txt')));
        static::assertNull($inner->status(path('memory://a.txt')));
    }
}
