<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_array, ref};
use Flow\ETL\Tests\FlowTestCase;

final class EnsureStartTest extends FlowTestCase
{
    public function test_ensure_start_chaining_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['command' => 'systemctl restart nginx'],
                ['command' => 'apt update'],
                ['command' => 'sudo systemctl stop apache'],
            ]))
            ->withEntry('safe_command', ref('command')->ensureStart('sudo ')->upper());

        self::assertEquals(
            [
                ['command' => 'systemctl restart nginx', 'safe_command' => 'SUDO SYSTEMCTL RESTART NGINX'],
                ['command' => 'apt update', 'safe_command' => 'SUDO APT UPDATE'],
                ['command' => 'sudo systemctl stop apache', 'safe_command' => 'SUDO SYSTEMCTL STOP APACHE'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_start_in_dataframe_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['url' => 'example.com', 'prefix' => 'https://'],
                ['url' => 'https://github.com', 'prefix' => 'https://'],
                ['url' => 'ftp://files.example.com', 'prefix' => 'https://'],
            ]))
            ->withEntry('normalized_url', ref('url')->ensureStart(ref('prefix')));

        self::assertEquals(
            [
                ['url' => 'example.com', 'prefix' => 'https://', 'normalized_url' => 'https://example.com'],
                ['url' => 'https://github.com', 'prefix' => 'https://', 'normalized_url' => 'https://github.com'],
                ['url' => 'ftp://files.example.com', 'prefix' => 'https://', 'normalized_url' => 'https://ftp://files.example.com'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_start_in_filtering_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['url' => 'example.com'],
                ['url' => 'https://secure.com'],
                ['url' => 'http://insecure.com'],
            ]))
            ->withEntry('normalized_url', ref('url')->ensureStart('https://'))
            ->filter(ref('normalized_url')->startsWith('https://'));

        self::assertEquals(
            [
                ['url' => 'example.com', 'normalized_url' => 'https://example.com'],
                ['url' => 'https://secure.com', 'normalized_url' => 'https://secure.com'],
                ['url' => 'http://insecure.com', 'normalized_url' => 'https://http://insecure.com'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_start_performance_with_large_dataset() : void
    {
        $data = [];

        for ($i = 0; $i < 1000; $i++) {
            $data[] = ['url' => "example{$i}.com"];
        }

        $df = df()
            ->from(from_array($data))
            ->withEntry('full_url', ref('url')->ensureStart('https://'));

        $result = $df->fetch()->toArray();

        self::assertCount(1000, $result);
        self::assertEquals('https://example0.com', $result[0]['full_url']);
        self::assertEquals('https://example999.com', $result[999]['full_url']);
    }

    public function test_ensure_start_with_aggregation_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['category' => 'tech', 'title' => 'PHP 8 Features'],
                ['category' => 'business', 'title' => 'Market Analysis'],
                ['category' => 'tech', 'title' => '[TECH] New Framework'],
            ]))
            ->withEntry('prefixed_title', ref('title')->ensureStart(ref('category')->upper()->append('] ')));

        self::assertEquals(
            [
                ['category' => 'tech', 'title' => 'PHP 8 Features', 'prefixed_title' => 'TECH] PHP 8 Features'],
                ['category' => 'business', 'title' => 'Market Analysis', 'prefixed_title' => 'BUSINESS] Market Analysis'],
                ['category' => 'tech', 'title' => '[TECH] New Framework', 'prefixed_title' => '[TECH] New Framework'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_start_with_constant_prefix() : void
    {
        $df = df()
            ->from(from_array([
                ['path' => 'var/log/app.log'],
                ['path' => '/var/log/error.log'],
                ['path' => 'tmp/cache.txt'],
            ]))
            ->withEntry('absolute_path', ref('path')->ensureStart('/'));

        self::assertEquals(
            [
                ['path' => 'var/log/app.log', 'absolute_path' => '/var/log/app.log'],
                ['path' => '/var/log/error.log', 'absolute_path' => '/var/log/error.log'],
                ['path' => 'tmp/cache.txt', 'absolute_path' => '/tmp/cache.txt'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_start_with_empty_and_null_prefixes() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => 'hello', 'prefix' => ''],
                ['text' => 'world', 'prefix' => null],
                ['text' => 'test', 'prefix' => 'prefix_'],
            ]))
            ->withEntry('processed_text', ref('text')->ensureStart(ref('prefix')));

        self::assertEquals(
            [
                ['text' => 'hello', 'prefix' => '', 'processed_text' => 'hello'],
                ['text' => 'world', 'prefix' => null, 'processed_text' => 'world'],
                ['text' => 'test', 'prefix' => 'prefix_', 'processed_text' => 'prefix_test'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_start_with_multiple_transformations() : void
    {
        $df = df()
            ->from(from_array([
                ['css_class' => 'button-primary', 'id_element' => 'main-content'],
                ['css_class' => '.button-secondary', 'id_element' => '#sidebar'],
                ['css_class' => 'link', 'id_element' => 'footer'],
            ]))
            ->withEntry('normalized_css_class', ref('css_class')->ensureStart('.'))
            ->withEntry('normalized_id_element', ref('id_element')->ensureStart('#'));

        self::assertEquals(
            [
                ['css_class' => 'button-primary', 'id_element' => 'main-content', 'normalized_css_class' => '.button-primary', 'normalized_id_element' => '#main-content'],
                ['css_class' => '.button-secondary', 'id_element' => '#sidebar', 'normalized_css_class' => '.button-secondary', 'normalized_id_element' => '#sidebar'],
                ['css_class' => 'link', 'id_element' => 'footer', 'normalized_css_class' => '.link', 'normalized_id_element' => '#footer'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_start_with_null_values() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => 'hello world'],
                ['text' => null],
                ['text' => 'test string'],
            ]))
            ->withEntry('prefixed_text', ref('text')->ensureStart('>>> '));

        self::assertEquals(
            [
                ['text' => 'hello world', 'prefixed_text' => '>>> hello world'],
                ['text' => null, 'prefixed_text' => null],
                ['text' => 'test string', 'prefixed_text' => '>>> test string'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_start_with_unicode_content() : void
    {
        $df = df()
            ->from(from_array([
                ['greeting' => 'world', 'prefix' => 'नमस्ते '],
                ['greeting' => 'नमस्ते world', 'prefix' => 'नमस्ते '],
                ['greeting' => 'hello', 'prefix' => '🚀 '],
            ]))
            ->withEntry('full_greeting', ref('greeting')->ensureStart(ref('prefix')));

        self::assertEquals(
            [
                ['greeting' => 'world', 'prefix' => 'नमस्ते ', 'full_greeting' => 'नमस्ते world'],
                ['greeting' => 'नमस्ते world', 'prefix' => 'नमस्ते ', 'full_greeting' => 'नमस्ते world'],
                ['greeting' => 'hello', 'prefix' => '🚀 ', 'full_greeting' => '🚀 hello'],
            ],
            $df->fetch()
        );
    }
}
