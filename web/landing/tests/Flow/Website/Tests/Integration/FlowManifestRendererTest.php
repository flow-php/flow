<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration;

use Flow\Website\Service\Manifest\Manifest;
use Flow\Website\Service\Markdown\FlowManifestRenderer;
use League\CommonMark\CommonMarkConverter;
use League\CommonMark\Event\DocumentParsedEvent;
use League\CommonMark\Extension\Table\TableExtension;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function file_put_contents;
use function json_encode;
use function strpos;
use function sys_get_temp_dir;
use function tempnam;
use function unlink;

final class FlowManifestRendererTest extends TestCase
{
    public function test_groups_packages_by_type_in_predefined_order(): void
    {
        $html = $this->render([
            ['name' => 'flow-php/etl-adapter-csv', 'path' => 'src/adapter/etl-adapter-csv', 'type' => 'adapter'],
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
            ['name' => 'flow-php/array-dot', 'path' => 'src/lib/array-dot', 'type' => 'lib'],
        ]);

        $core = strpos($html, '<strong>Core</strong>');
        $adapters = strpos($html, '<strong>Adapters</strong>');
        $libraries = strpos($html, '<strong>Libraries</strong>');

        static::assertIsInt($core);
        static::assertIsInt($adapters);
        static::assertIsInt($libraries);
        static::assertLessThan($adapters, $core);
        static::assertLessThan($libraries, $adapters);
    }

    public function test_ignores_unknown_package_types(): void
    {
        $html = $this->render([
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
            ['name' => 'flow-php/mystery', 'path' => 'src/mystery', 'type' => 'something-else'],
        ]);

        static::assertStringContainsString('flow-php/etl', $html);
        static::assertStringNotContainsString('flow-php/mystery', $html);
    }

    public function test_paragraph_with_marker_and_extra_text_is_left_alone(): void
    {
        $html = $this->render([[
            'name' => 'flow-php/etl',
            'path' => 'src/core/etl',
            'type' => 'core',
        ]], "Inline mention of [FLOW_MANIFEST] inside prose.\n");

        static::assertStringContainsString('[FLOW_MANIFEST]', $html);
        static::assertStringNotContainsString('<table>', $html);
    }

    public function test_paragraph_with_only_marker_is_replaced(): void
    {
        $html = $this->render([[
            'name' => 'flow-php/etl',
            'path' => 'src/core/etl',
            'type' => 'core',
        ]], "## Heading\n\n[FLOW_MANIFEST]\n\nAfter.\n");

        static::assertStringContainsString('<table>', $html);
        static::assertStringContainsString('<h2>Heading</h2>', $html);
        static::assertStringContainsString('<p>After.</p>', $html);
        static::assertStringNotContainsString('[FLOW_MANIFEST]', $html);
    }

    public function test_renders_install_link_pointing_at_per_package_doc(): void
    {
        $html = $this->render([
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
        ]);

        static::assertStringContainsString('<a href="/documentation/installation/packages/etl.md">Install</a>', $html);
    }

    public function test_renders_packagist_badges_with_correct_alt_text(): void
    {
        $html = $this->render([
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
        ]);

        static::assertStringContainsString('href="https://packagist.org/packages/flow-php/etl"', $html);
        static::assertStringContainsString('src="https://poser.pugx.org/flow-php/etl/downloads"', $html);
        static::assertStringContainsString('src="https://poser.pugx.org/flow-php/etl/v/stable"', $html);
        static::assertStringContainsString('alt="Total Downloads"', $html);
        static::assertStringContainsString('alt="Latest Stable Version"', $html);
    }

    public function test_renders_table_header_columns(): void
    {
        $html = $this->render([
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
        ]);

        static::assertStringContainsString('<th>Package</th>', $html);
        static::assertStringContainsString('<th>Installation</th>', $html);
        static::assertStringContainsString('<th>Downloads</th>', $html);
        static::assertStringContainsString('<th>Version</th>', $html);
    }

    public function test_throws_when_manifest_file_is_missing(): void
    {
        $converter = new CommonMarkConverter();
        $converter
            ->getEnvironment()
            ->addExtension(new TableExtension())
            ->addEventListener(
                DocumentParsedEvent::class,
                new FlowManifestRenderer(new Manifest('/nonexistent/manifest.json')),
            );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Flow manifest not found');

        $converter->convert('[FLOW_MANIFEST]');
    }

    /**
     * @param list<array{name: string, path: string, type: string}> $packages
     */
    private function render(array $packages, string $markdown = "[FLOW_MANIFEST]\n"): string
    {
        $manifestPath = tempnam(sys_get_temp_dir(), 'flow-manifest-');

        if ($manifestPath === false) {
            self::fail('Failed to create temp manifest file.');
        }

        file_put_contents($manifestPath, json_encode(['packages' => $packages]));

        try {
            $converter = new CommonMarkConverter();
            $converter
                ->getEnvironment()
                ->addExtension(new TableExtension())
                ->addEventListener(DocumentParsedEvent::class, new FlowManifestRenderer(new Manifest($manifestPath)));

            return (string) $converter->convert($markdown);
        } finally {
            @unlink($manifestPath);
        }
    }
}
