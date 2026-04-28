<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration;

use Flow\Website\Service\Markdown\FlowManifestRenderer;
use League\CommonMark\CommonMarkConverter;
use League\CommonMark\Event\DocumentParsedEvent;
use League\CommonMark\Extension\Table\TableExtension;
use PHPUnit\Framework\TestCase;

final class FlowManifestRendererTest extends TestCase
{
    public function test_groups_packages_by_type_in_predefined_order() : void
    {
        $html = $this->render([
            ['name' => 'flow-php/etl-adapter-csv', 'path' => 'src/adapter/etl-adapter-csv', 'type' => 'adapter'],
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
            ['name' => 'flow-php/array-dot', 'path' => 'src/lib/array-dot', 'type' => 'lib'],
        ]);

        $core = \strpos($html, '<strong>Core</strong>');
        $adapters = \strpos($html, '<strong>Adapters</strong>');
        $libraries = \strpos($html, '<strong>Libraries</strong>');

        self::assertIsInt($core);
        self::assertIsInt($adapters);
        self::assertIsInt($libraries);
        self::assertLessThan($adapters, $core);
        self::assertLessThan($libraries, $adapters);
    }

    public function test_ignores_unknown_package_types() : void
    {
        $html = $this->render([
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
            ['name' => 'flow-php/mystery', 'path' => 'src/mystery', 'type' => 'something-else'],
        ]);

        self::assertStringContainsString('flow-php/etl', $html);
        self::assertStringNotContainsString('flow-php/mystery', $html);
    }

    public function test_paragraph_with_marker_and_extra_text_is_left_alone() : void
    {
        $html = $this->render(
            [['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core']],
            "Inline mention of [FLOW_MANIFEST] inside prose.\n",
        );

        self::assertStringContainsString('[FLOW_MANIFEST]', $html);
        self::assertStringNotContainsString('<table>', $html);
    }

    public function test_paragraph_with_only_marker_is_replaced() : void
    {
        $html = $this->render(
            [['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core']],
            "## Heading\n\n[FLOW_MANIFEST]\n\nAfter.\n",
        );

        self::assertStringContainsString('<table>', $html);
        self::assertStringContainsString('<h2>Heading</h2>', $html);
        self::assertStringContainsString('<p>After.</p>', $html);
        self::assertStringNotContainsString('[FLOW_MANIFEST]', $html);
    }

    public function test_renders_install_link_pointing_at_per_package_doc() : void
    {
        $html = $this->render([
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
        ]);

        self::assertStringContainsString('<a href="/documentation/installation/packages/etl.md">Install</a>', $html);
    }

    public function test_renders_packagist_badges_with_correct_alt_text() : void
    {
        $html = $this->render([
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
        ]);

        self::assertStringContainsString('href="https://packagist.org/packages/flow-php/etl"', $html);
        self::assertStringContainsString('src="https://poser.pugx.org/flow-php/etl/downloads"', $html);
        self::assertStringContainsString('src="https://poser.pugx.org/flow-php/etl/v/stable"', $html);
        self::assertStringContainsString('alt="Total Downloads"', $html);
        self::assertStringContainsString('alt="Latest Stable Version"', $html);
    }

    public function test_renders_table_header_columns() : void
    {
        $html = $this->render([
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
        ]);

        self::assertStringContainsString('<th>Package</th>', $html);
        self::assertStringContainsString('<th>Installation</th>', $html);
        self::assertStringContainsString('<th>Downloads</th>', $html);
        self::assertStringContainsString('<th>Version</th>', $html);
    }

    public function test_throws_when_manifest_file_is_missing() : void
    {
        $converter = new CommonMarkConverter();
        $converter->getEnvironment()
            ->addExtension(new TableExtension())
            ->addEventListener(DocumentParsedEvent::class, new FlowManifestRenderer('/nonexistent/manifest.json'));

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Flow manifest not found');

        $converter->convert('[FLOW_MANIFEST]');
    }

    /**
     * @param list<array{name: string, path: string, type: string}> $packages
     */
    private function render(array $packages, string $markdown = "[FLOW_MANIFEST]\n") : string
    {
        $manifestPath = \tempnam(\sys_get_temp_dir(), 'flow-manifest-');

        if ($manifestPath === false) {
            self::fail('Failed to create temp manifest file.');
        }

        \file_put_contents($manifestPath, \json_encode(['packages' => $packages]));

        try {
            $converter = new CommonMarkConverter();
            $converter->getEnvironment()
                ->addExtension(new TableExtension())
                ->addEventListener(DocumentParsedEvent::class, new FlowManifestRenderer($manifestPath));

            return (string) $converter->convert($markdown);
        } finally {
            @\unlink($manifestPath);
        }
    }
}
