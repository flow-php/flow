<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Context\CliCommandContext;

use PHPUnit\Framework\TestCase;

final class FstabResolverTest extends TestCase
{
    private CliCommandContext $context;

    protected function setUp() : void
    {
        $this->context = new CliCommandContext();
    }

    protected function tearDown() : void
    {
        $this->context->cleanup();
    }

    public function test_available_fstabs_lists_all_provided_services() : void
    {
        $resolver = $this->context->resolver(null, $this->context->secondaryMemoryOnly());

        self::assertSame(['default', 'secondary'], $resolver->availableFstabs());
    }

    public function test_default_fstab_name_exposed() : void
    {
        self::assertSame('default', $this->context->resolver()->defaultFstabName());
    }

    public function test_parse_uri_accepts_valid_uri() : void
    {
        self::assertSame('memory://data/file.txt', $this->context->resolver()->parseUri('memory://data/file.txt')->uri());
    }

    public function test_parse_uri_assumes_local_filesystem_for_absolute_path() : void
    {
        $path = $this->context->resolver()->parseUri('/tmp/file.txt');
        self::assertSame('file', $path->protocol()->name);
        self::assertSame('/tmp/file.txt', $path->path());
    }

    public function test_parse_uri_resolves_relative_path_against_cwd() : void
    {
        $cwd = (string) \getcwd();
        $path = $this->context->resolver()->parseUri('file.txt');
        self::assertSame('file', $path->protocol()->name);
        self::assertSame($cwd . '/file.txt', $path->path());
    }

    public function test_resolve_returns_default_when_name_null() : void
    {
        $default = $this->context->defaultTable();
        self::assertSame($default, $this->context->resolver($default)->resolve(null));
    }

    public function test_resolve_returns_requested_fstab() : void
    {
        $secondary = $this->context->secondaryMemoryOnly();
        $resolver = $this->context->resolver(null, $secondary);

        self::assertSame($secondary, $resolver->resolve('secondary'));
    }

    public function test_resolve_throws_on_unknown_fstab() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown fstab "nope"');

        $this->context->resolver()->resolve('nope');
    }
}
