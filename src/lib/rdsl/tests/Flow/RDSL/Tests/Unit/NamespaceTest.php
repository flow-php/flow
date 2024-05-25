<?php

declare(strict_types=1);

namespace Flow\RDSL\Tests\Unit;

use Flow\RDSL\AccessControl\AllowAll;
use Flow\RDSL\DSLNamespace;
use Flow\RDSL\Exception\InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class NamespaceTest extends TestCase
{
    public static function invalid_namespaces_provider() : \Generator
    {
        yield ['Foo\\'];
        yield ['Foo'];
        yield ['\Foo\Bar\Baz\\'];
    }

    public static function valid_namespaces_provider() : \Generator
    {
        yield ['\\'];
        yield ['\Foo'];
        yield ['\Foo\Bar'];
        yield ['\Foo\Bar\Baz'];
    }

    #[DataProvider('invalid_namespaces_provider')]
    public function test_invalid_namespaces(string $ns) : void
    {
        $this->expectException(InvalidArgumentException::class);
        new DSLNamespace($ns, new AllowAll());
    }

    #[DataProvider('valid_namespaces_provider')]
    public function test_valid_namespaces(string $ns) : void
    {
        self::assertInstanceOf(DSLNamespace::class, new DSLNamespace($ns, new AllowAll()));
    }
}
