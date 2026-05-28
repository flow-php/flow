<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use Flow\Website\Service\Manifest\Manifest;
use League\CommonMark\Event\DocumentParsedEvent;
use League\CommonMark\Extension\CommonMark\Node\Block\HtmlBlock;
use League\CommonMark\Node\Block\Paragraph;
use League\CommonMark\Node\Inline\Text;

use function array_key_exists;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;

final readonly class FlowPackageNavRenderer
{
    private const string PLACEHOLDER_COMPONENT = '[PACKAGE_NAV]';

    private const string PLACEHOLDER_INSTALL = '[PACKAGE_NAV:install]';

    private const array TYPE_AUTOLINKS = [
        'core' => ['packagist', 'github', 'installation'],
        'cli' => ['packagist', 'github', 'installation'],
        'adapter' => ['packagist', 'github', 'installation'],
        'lib' => ['packagist', 'github', 'installation'],
        'bridge' => ['packagist', 'github', 'installation'],
        'extension' => ['github', 'installation'],
    ];

    private const array TYPE_AUTOLINKS_INSTALL = [
        'core' => ['documentation', 'packagist', 'github'],
        'cli' => ['documentation', 'packagist', 'github'],
        'adapter' => ['documentation', 'packagist', 'github'],
        'lib' => ['documentation', 'packagist', 'github'],
        'bridge' => ['documentation', 'packagist', 'github'],
        'extension' => ['documentation', 'github'],
    ];

    /** Maps `manifest.json` `type` to its docs folder name. */
    private const array TYPE_FOLDER = [
        'core' => 'core',
        'cli' => 'cli',
        'adapter' => 'adapters',
        'lib' => 'libs',
        'bridge' => 'bridges',
        'extension' => 'extensions',
    ];

    public function __construct(
        private Manifest $manifest,
    ) {}

    public function __invoke(DocumentParsedEvent $event): void
    {
        $document = $event->getDocument();
        $frontMatter = type_map(type_string(), type_mixed())->assert($document->data->get('front_matter', []));
        $packageName = array_key_exists('package', $frontMatter) && is_string($frontMatter['package'])
            ? $frontMatter['package']
            : null;

        $walker = $document->walker();

        while ($walkEvent = $walker->next()) {
            if (!$walkEvent->isEntering()) {
                continue;
            }

            $node = $walkEvent->getNode();

            if (!$node instanceof Text) {
                continue;
            }

            $literal = $node->getLiteral();

            if ($literal !== self::PLACEHOLDER_COMPONENT && $literal !== self::PLACEHOLDER_INSTALL) {
                continue;
            }

            $owner = $node->parent();

            if (!$owner instanceof Paragraph) {
                continue;
            }

            if ($owner->firstChild() !== $node || $node->next() !== null) {
                continue;
            }

            $package = $packageName !== null ? $this->manifest->byName($packageName) : null;

            if ($package === null) {
                $owner->detach();

                continue;
            }

            $variant = $literal === self::PLACEHOLDER_INSTALL ? 'install' : 'component';

            $block = new HtmlBlock(HtmlBlock::TYPE_6_BLOCK_ELEMENT);
            $block->setLiteral($this->buildNavHtml($package, $variant));
            $owner->insertBefore($block);
            $owner->detach();
        }
    }

    /**
     * @param array<string, mixed> $package
     */
    private function buildNavHtml(array $package, string $variant): string
    {
        $name = (string) $package['name'];
        $type = (string) $package['type'];
        $slug = $this->slug($name);
        $typeFolder = self::TYPE_FOLDER[$type] ?? $type;
        $links = is_array($package['links'] ?? null) ? $package['links'] : [];

        $items = [];
        $autolinks = $variant === 'install'
            ? self::TYPE_AUTOLINKS_INSTALL[$type] ?? ['documentation', 'packagist', 'github']
            : self::TYPE_AUTOLINKS[$type] ?? ['packagist', 'github', 'installation'];

        if (
            in_array('documentation', $autolinks, true)
            && isset($links['documentation'])
            && is_string($links['documentation'])
        ) {
            $items[] = $this->item($links['documentation'], 'Documentation');
        }

        if (in_array('packagist', $autolinks, true)) {
            $items[] = $this->item(href: 'https://packagist.org/packages/' . $name, label: 'Packagist', external: true);
        }

        if (in_array('github', $autolinks, true)) {
            $items[] = $this->item(href: 'https://github.com/' . $name, label: 'GitHub', external: true);
        }

        if (in_array('installation', $autolinks, true)) {
            $items[] = $this->item(href: '/documentation/installation/packages/' . $slug, label: 'Installation');
        }

        if (isset($links['architecture']) && is_string($links['architecture'])) {
            $items[] = $this->item($links['architecture'], 'Architecture');
        }

        if (isset($links['api']) && is_string($links['api'])) {
            $items[] = $this->item($links['api'], 'API Reference');
        }

        if (isset($links['dsl']) && is_string($links['dsl'])) {
            $items[] = $this->item($links['dsl'], 'DSL');
        }

        if (isset($links['files']) && is_string($links['files'])) {
            $items[] = $this->item($links['files'], 'Files');
        }

        unset($typeFolder); // currently derived but reserved for future link conventions

        return '<nav aria-label="Package links" class="package-nav">' . implode('', $items) . '</nav>';
    }

    private function item(string $href, string $label, bool $external = false): string
    {
        $hrefAttr = htmlspecialchars($href, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
        $labelHtml = htmlspecialchars($label, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
        $extra = $external ? ' target="_blank" rel="noopener"' : '';
        $arrow = $external ? ' <span aria-hidden="true">↗</span>' : '';

        return '<a href="' . $hrefAttr . '"' . $extra . '>' . $labelHtml . $arrow . '</a>';
    }

    private function slug(string $composerName): string
    {
        $slash = strrpos($composerName, '/');

        return $slash === false ? $composerName : substr($composerName, $slash + 1);
    }
}
