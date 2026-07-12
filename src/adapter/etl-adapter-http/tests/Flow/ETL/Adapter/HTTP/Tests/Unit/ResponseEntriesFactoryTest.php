<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit;

use Flow\ETL\Adapter\Http\ResponseEntriesFactory;
use Flow\ETL\Row\Entry\HTMLEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\XMLEntry;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use Nyholm\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;
use Psr\Http\Message\ResponseInterface;

use function json_encode;

final class ResponseEntriesFactoryTest extends FlowTestCase
{
    public static function responses(): Generator
    {
        yield 'uses JsonEntry for response body when Content-Type header is application/json' => [
            JsonEntry::class,
            new Response(headers: ['Content-Type' => 'application/json'], body: json_encode([
                'status' => 'success',
            ], JSON_THROW_ON_ERROR)),
        ];

        yield 'uses XmlEntry for response body when Content-Type header is application/xml' => [
            XMLEntry::class,
            new Response(headers: ['Content-Type' => 'application/xml'], body: '<root><foo baz="buz">bar</foo></root>'),
        ];

        yield 'uses StringEntry for response body when no Content-Type header was provided' => [
            StringEntry::class,
            new Response(body: '<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p>3</p></body></html>'),
        ];
    }

    /**
     * @param class-string $entryClass
     */
    #[DataProvider('responses')]
    public function test_uses_expected_entry_for_response_body(string $entryClass, ResponseInterface $response): void
    {
        $entryFactory = new ResponseEntriesFactory();

        static::assertInstanceOf($entryClass, $entryFactory->create($response)->get('response_body'));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_uses_html_entry_for_response_body_on_newer_php(): void
    {
        $entryFactory = new ResponseEntriesFactory();

        $response = new Response(headers: [
            'Content-Type' => 'text/html',
        ], body: '<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p>3</p></body></html>');

        static::assertInstanceOf(HTMLEntry::class, $entryFactory->create($response)->get('response_body'));
    }

    #[RequiresPhp('< 8.4.0')]
    public function test_uses_string_entry_for_response_body_on_older_php(): void
    {
        $entryFactory = new ResponseEntriesFactory();

        $response = new Response(headers: [
            'Content-Type' => 'text/html',
        ], body: '<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p>3</p></body></html>');

        static::assertInstanceOf(StringEntry::class, $entryFactory->create($response)->get('response_body'));
    }
}
