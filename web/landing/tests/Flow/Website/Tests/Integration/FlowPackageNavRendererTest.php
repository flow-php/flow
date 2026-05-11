<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration;

use Flow\Website\Service\Manifest\Manifest;
use Flow\Website\Service\Markdown\FlowPackageNavRenderer;
use League\CommonMark\CommonMarkConverter;
use League\CommonMark\Event\DocumentParsedEvent;
use League\CommonMark\Extension\FrontMatter\FrontMatterExtension;
use PHPUnit\Framework\TestCase;

final class FlowPackageNavRendererTest extends TestCase
{
    public function test_component_variant_omits_installation_link_when_links_documentation_present(): void
    {
        $html = $this->render([[
            'name' => 'flow-php/etl-adapter-csv',
            'path' => 'src/adapter/etl-adapter-csv',
            'type' => 'adapter',
            'links' => ['documentation' => '/documentation/components/adapters/csv'],
        ]], "---\npackage: flow-php/etl-adapter-csv\n---\n\n[PACKAGE_NAV]\n");

        // Component variant should not surface the documentation link
        static::assertStringNotContainsString('href="/documentation/components/adapters/csv"', $html);
    }

    public function test_component_variant_renders_packagist_github_and_installation_links(): void
    {
        $html = $this->render([[
            'name' => 'flow-php/etl-adapter-csv',
            'path' => 'src/adapter/etl-adapter-csv',
            'type' => 'adapter',
            'links' => ['documentation' => '/documentation/components/adapters/csv'],
        ]], "---\npackage: flow-php/etl-adapter-csv\n---\n\n[PACKAGE_NAV]\n");

        static::assertStringContainsString('href="https://packagist.org/packages/flow-php/etl-adapter-csv"', $html);
        static::assertStringContainsString('href="https://github.com/flow-php/etl-adapter-csv"', $html);
        static::assertStringContainsString('href="/documentation/installation/packages/etl-adapter-csv"', $html);
        static::assertStringContainsString('>Installation</a>', $html);
        static::assertStringNotContainsString('>Documentation</a>', $html);
    }

    public function test_extension_component_variant_omits_packagist(): void
    {
        $html = $this->render([[
            'name' => 'flow-php/arrow-ext',
            'path' => 'src/extension/arrow-ext',
            'type' => 'extension',
            'links' => ['documentation' => '/documentation/components/extensions/arrow-ext'],
        ]], "---\npackage: flow-php/arrow-ext\n---\n\n[PACKAGE_NAV]\n");

        static::assertStringNotContainsString('packagist.org/packages/flow-php/arrow-ext', $html);
        static::assertStringContainsString('href="https://github.com/flow-php/arrow-ext"', $html);
        static::assertStringContainsString('href="/documentation/installation/packages/arrow-ext"', $html);
    }

    public function test_install_variant_extension_omits_packagist(): void
    {
        $html = $this->render([[
            'name' => 'flow-php/arrow-ext',
            'path' => 'src/extension/arrow-ext',
            'type' => 'extension',
            'links' => ['documentation' => '/documentation/components/extensions/arrow-ext'],
        ]], "---\npackage: flow-php/arrow-ext\n---\n\n[PACKAGE_NAV:install]\n");

        static::assertStringNotContainsString('packagist.org/packages/flow-php/arrow-ext', $html);
        static::assertStringContainsString('href="https://github.com/flow-php/arrow-ext"', $html);
        static::assertStringContainsString('href="/documentation/components/extensions/arrow-ext"', $html);
    }

    public function test_install_variant_includes_optional_manifest_links(): void
    {
        $html = $this->render([
            [
                'name' => 'flow-php/etl',
                'path' => 'src/core/etl',
                'type' => 'core',
                'links' => [
                    'documentation' => '/documentation/components/core/core',
                    'api' => '/documentation/api/core',
                    'dsl' => '/documentation/api/core/namespaces/flow-etl-dsl.html',
                    'files' => '/documentation/api/core/indices/files.html',
                    'architecture' => '/documentation/components/core/architecture',
                ],
            ],
        ], "---\npackage: flow-php/etl\n---\n\n[PACKAGE_NAV:install]\n");

        static::assertStringContainsString('>Architecture</a>', $html);
        static::assertStringContainsString('>API Reference</a>', $html);
        static::assertStringContainsString('>DSL</a>', $html);
        static::assertStringContainsString('>Files</a>', $html);
    }

    public function test_install_variant_renders_documentation_packagist_and_github(): void
    {
        $html = $this->render([[
            'name' => 'flow-php/etl-adapter-csv',
            'path' => 'src/adapter/etl-adapter-csv',
            'type' => 'adapter',
            'links' => ['documentation' => '/documentation/components/adapters/csv'],
        ]], "---\npackage: flow-php/etl-adapter-csv\n---\n\n[PACKAGE_NAV:install]\n");

        static::assertStringContainsString('href="/documentation/components/adapters/csv"', $html);
        static::assertStringContainsString('>Documentation</a>', $html);
        static::assertStringContainsString('href="https://packagist.org/packages/flow-php/etl-adapter-csv"', $html);
        static::assertStringContainsString('href="https://github.com/flow-php/etl-adapter-csv"', $html);
        static::assertStringNotContainsString('href="/documentation/installation/packages/etl-adapter-csv"', $html);
        static::assertStringNotContainsString('>Installation</a>', $html);
    }

    public function test_install_variant_skips_documentation_when_link_missing(): void
    {
        $html = $this->render([[
            'name' => 'flow-php/etl-adapter-csv',
            'path' => 'src/adapter/etl-adapter-csv',
            'type' => 'adapter',
        ]], "---\npackage: flow-php/etl-adapter-csv\n---\n\n[PACKAGE_NAV:install]\n");

        static::assertStringNotContainsString('>Documentation</a>', $html);
        static::assertStringContainsString('href="https://packagist.org/packages/flow-php/etl-adapter-csv"', $html);
    }

    public function test_paragraph_with_marker_and_extra_text_is_left_alone(): void
    {
        $html = $this->render([[
            'name' => 'flow-php/etl',
            'path' => 'src/core/etl',
            'type' => 'core',
            'links' => ['documentation' => '/documentation/components/core/core'],
        ]], "---\npackage: flow-php/etl\n---\n\nInline mention of [PACKAGE_NAV:install] inside prose.\n");

        static::assertStringContainsString('[PACKAGE_NAV:install]', $html);
        static::assertStringNotContainsString('class="package-nav"', $html);
    }

    public function test_placeholder_with_unknown_package_is_removed(): void
    {
        $html = $this->render([[
            'name' => 'flow-php/etl',
            'path' => 'src/core/etl',
            'type' => 'core',
            'links' => ['documentation' => '/documentation/components/core/core'],
        ]], "---\npackage: flow-php/unknown\n---\n\n[PACKAGE_NAV:install]\n");

        static::assertStringNotContainsString('[PACKAGE_NAV:install]', $html);
        static::assertStringNotContainsString('class="package-nav"', $html);
    }

    public function test_placeholder_without_package_frontmatter_is_removed(): void
    {
        $html = $this->render([[
            'name' => 'flow-php/etl',
            'path' => 'src/core/etl',
            'type' => 'core',
            'links' => ['documentation' => '/documentation/components/core/core'],
        ]], "[PACKAGE_NAV:install]\n");

        static::assertStringNotContainsString('[PACKAGE_NAV:install]', $html);
        static::assertStringNotContainsString('class="package-nav"', $html);
    }

    /**
     * @param list<array<string, mixed>> $packages
     */
    private function render(array $packages, string $markdown): string
    {
        $manifestPath = \tempnam(\sys_get_temp_dir(), 'flow-manifest-');

        if ($manifestPath === false) {
            self::fail('Failed to create temp manifest file.');
        }

        \file_put_contents($manifestPath, \json_encode(['packages' => $packages]));

        try {
            $converter = new CommonMarkConverter();
            $converter
                ->getEnvironment()
                ->addExtension(new FrontMatterExtension())
                ->addEventListener(DocumentParsedEvent::class, new FlowPackageNavRenderer(new Manifest($manifestPath)));

            return (string) $converter->convert($markdown);
        } finally {
            @\unlink($manifestPath);
        }
    }
}
