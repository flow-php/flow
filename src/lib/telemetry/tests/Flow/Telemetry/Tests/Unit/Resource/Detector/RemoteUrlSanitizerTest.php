<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Resource\Detector;

use Flow\Telemetry\Resource\Detector\RemoteUrlSanitizer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class RemoteUrlSanitizerTest extends TestCase
{
    public static function provideUrls(): Generator
    {
        yield 'strips username and password' => [
            'https://user:secret@github.com/flow-php/repo.git',
            'https://github.com/flow-php/repo.git',
        ];

        yield 'strips username only' => [
            'https://user@github.com/flow-php/repo.git',
            'https://github.com/flow-php/repo.git',
        ];

        yield 'preserves port and path while stripping credentials, query and fragment' => [
            'https://user:secret@github.com:8443/flow-php/repo.git?ref=main#readme',
            'https://github.com:8443/flow-php/repo.git',
        ];

        yield 'strips query and fragment from a credential-free url' => [
            'https://github.com/flow-php/repo.git?token=secret#section',
            'https://github.com/flow-php/repo.git',
        ];

        yield 'leaves scp-like ssh remote untouched' => [
            'git@github.com:flow-php/repo.git',
            'git@github.com:flow-php/repo.git',
        ];

        yield 'leaves clean https url untouched' => [
            'https://github.com/flow-php/repo.git',
            'https://github.com/flow-php/repo.git',
        ];

        yield 'leaves unparseable url untouched' => [
            'not a url',
            'not a url',
        ];
    }

    #[DataProvider('provideUrls')]
    public function test_sanitize(string $url, string $expected): void
    {
        $sanitizer = new RemoteUrlSanitizer();

        static::assertSame($expected, $sanitizer->sanitize($url));
    }
}
