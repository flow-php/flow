<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Sanitizer;

use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\url_sanitizer;

final class UrlSanitizerTest extends TestCase
{
    #[TestWith(['X-Amz-Signature'])]
    #[TestWith(['sig'])]
    #[TestWith(['access_token'])]
    #[TestWith(['client_secret'])]
    #[TestWith(['code'])]
    #[TestWith(['token'])]
    #[TestWith(['api_key'])]
    #[TestWith(['key'])]
    #[TestWith(['password'])]
    #[TestWith(['PHPSESSID'])]
    public function test_default_list_redacts_semantic_convention_oauth_and_credential_keys(string $key): void
    {
        static::assertSame(
            'https://h/x?' . $key . '=REDACTED&fields=id',
            url_sanitizer()->url('https://h/x?' . $key . '=S&fields=id'),
        );
    }

    public function test_empty_list_redacts_userinfo_only(): void
    {
        static::assertSame('https://REDACTED:REDACTED@h/x?sig=S', url_sanitizer([])->url('https://u:p@h/x?sig=S'));
    }

    public function test_message_keeps_message_when_url_is_absent(): void
    {
        static::assertSame('Connection refused.', url_sanitizer(['access_token'])->message(
            'Connection refused.',
            'https://h/x?access_token=T',
        ));
    }

    public function test_message_keeps_message_when_url_is_empty(): void
    {
        static::assertSame('HTTP 400 returned for "".', url_sanitizer(['access_token'])->message(
            'HTTP 400 returned for "".',
            '',
        ));
    }

    public function test_message_keeps_message_when_url_has_nothing_to_redact(): void
    {
        static::assertSame('HTTP 400 returned for "https://h/x?fields=id".', url_sanitizer(['access_token'])->message(
            'HTTP 400 returned for "https://h/x?fields=id".',
            'https://h/x?fields=id',
        ));
    }

    public function test_message_replaces_every_occurrence_of_url(): void
    {
        static::assertSame('HTTP 400 returned for "https://h/x?access_token=REDACTED", retry "https://h/x?access_token=REDACTED".', url_sanitizer([
            'access_token',
        ])->message(
            'HTTP 400 returned for "https://h/x?access_token=T", retry "https://h/x?access_token=T".',
            'https://h/x?access_token=T',
        ));
    }

    #[TestWith(['code=C&state=abc', 'code=REDACTED&state=abc'])]
    #[TestWith(['', ''])]
    #[TestWith(['state=abc&code=C', 'state=abc&code=REDACTED'])]
    #[TestWith(['code', 'code'])]
    public function test_query(string $query, string $expected): void
    {
        static::assertSame($expected, url_sanitizer(['code'])->query($query));
    }

    #[TestWith(['https://h/x?access_token=T&fields=id', 'https://h/x?access_token=REDACTED&fields=id'])]
    #[TestWith(['https://u:p@h/x?sig=S#frag', 'https://REDACTED:REDACTED@h/x?sig=REDACTED#frag'])]
    #[TestWith(['https://u@h/x', 'https://REDACTED@h/x'])]
    #[TestWith(['https://h/x?Sig=S', 'https://h/x?Sig=S'])]
    #[TestWith(['https://h/x?access%5Ftoken=T', 'https://h/x?access%5Ftoken=REDACTED'])]
    #[TestWith(['https://h/x?a[]=1&sig=S&sig=S2&flag', 'https://h/x?a[]=1&sig=REDACTED&sig=REDACTED&flag'])]
    #[TestWith(['https://h/x?sig=', 'https://h/x?sig=REDACTED'])]
    #[TestWith(['https://h/x?sig', 'https://h/x?sig'])]
    #[TestWith(['https://h/x#sig=S', 'https://h/x#sig=S'])]
    #[TestWith(['https://h/x#a?sig=S', 'https://h/x#a?sig=S'])]
    #[TestWith(['/relative?sig=S', '/relative?sig=REDACTED'])]
    #[TestWith(['https://h/x?sig=S=T', 'https://h/x?sig=REDACTED'])]
    #[TestWith(['https://h/x?sig=S?a=b', 'https://h/x?sig=REDACTED'])]
    #[TestWith(['https://h/x?q=a@b', 'https://h/x?q=a@b'])]
    #[TestWith(['https://h/x/u@h?q=1', 'https://h/x/u@h?q=1'])]
    #[TestWith(['mailto:u@h', 'mailto:u@h'])]
    #[TestWith(['xhttps://u@h/x', 'xhttps://REDACTED@h/x'])]
    #[TestWith(['1https://u@h/x', '1https://u@h/x'])]
    #[TestWith(['svn+ssh.v-1://u:p@h/x', 'svn+ssh.v-1://REDACTED:REDACTED@h/x'])]
    #[TestWith(['https://@h/x', 'https://REDACTED@h/x'])]
    public function test_url(string $url, string $expected): void
    {
        static::assertSame($expected, url_sanitizer(['access_token', 'sig'])->url($url));
    }
}
