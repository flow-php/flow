<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use League\CommonMark\Event\DocumentParsedEvent;
use League\CommonMark\Extension\CommonMark\Node\Inline\{Image, Link, Strong};
use League\CommonMark\Extension\Table\{Table, TableCell, TableRow, TableSection};
use League\CommonMark\Node\Block\Paragraph;
use League\CommonMark\Node\Inline\Text;

final class FlowManifestRenderer
{
    private const string PLACEHOLDER = '[FLOW_MANIFEST]';

    /**
     * Maps `manifest.json` `type` values to the section row label + render order.
     *
     * @var array<string, array{label: string, order: int}>
     */
    private const array TYPE_GROUPS = [
        'core' => ['label' => 'Core', 'order' => 0],
        'cli' => ['label' => 'CLI', 'order' => 1],
        'adapter' => ['label' => 'Adapters', 'order' => 2],
        'lib' => ['label' => 'Libraries', 'order' => 3],
        'bridge' => ['label' => 'Bridges', 'order' => 4],
        'extension' => ['label' => 'Extensions', 'order' => 5],
    ];

    /**
     * @var null|list<array{name: string, slug: string, type: string}>
     */
    private ?array $packagesCache = null;

    public function __construct(private readonly string $manifestPath)
    {
    }

    public function __invoke(DocumentParsedEvent $event) : void
    {
        $document = $event->getDocument();
        $walker = $document->walker();

        while ($walkEvent = $walker->next()) {
            if (!$walkEvent->isEntering()) {
                continue;
            }

            $node = $walkEvent->getNode();

            if (!$node instanceof Text) {
                continue;
            }

            if ($node->getLiteral() !== self::PLACEHOLDER) {
                continue;
            }

            $owner = $node->parent();

            if (!$owner instanceof Paragraph) {
                continue;
            }

            // Only fire when the paragraph is exactly the placeholder. Avoid
            // accidentally hijacking text that happens to equal the marker.
            if ($owner->firstChild() !== $node || $node->next() !== null) {
                continue;
            }

            $owner->insertBefore($this->buildTable());
            $owner->detach();
        }
    }

    private function buildBadgeCell(string $badgeUrl, string $badgeAlt, string $packagistUrl) : TableCell
    {
        $cell = new TableCell(TableCell::TYPE_DATA);
        $link = new Link($packagistUrl);
        $link->appendChild(new Image($badgeUrl, $badgeAlt));
        $cell->appendChild($link);

        return $cell;
    }

    private function buildBody() : TableSection
    {
        $body = new TableSection(TableSection::TYPE_BODY);

        $grouped = [];

        foreach ($this->packages() as $package) {
            $grouped[$package['type']][] = $package;
        }

        foreach (\array_keys(self::TYPE_GROUPS) as $type) {
            if (!\array_key_exists($type, $grouped)) {
                continue;
            }

            $body->appendChild($this->buildGroupHeadingRow(self::TYPE_GROUPS[$type]['label']));

            $items = $grouped[$type];
            \usort($items, static fn (array $a, array $b) : int => \strcmp($a['name'], $b['name']));

            foreach ($items as $package) {
                $body->appendChild($this->buildPackageRow($package));
            }
        }

        return $body;
    }

    private function buildGroupHeadingRow(string $label) : TableRow
    {
        $row = new TableRow();

        $first = new TableCell(TableCell::TYPE_DATA);
        $strong = new Strong();
        $strong->appendChild(new Text($label));
        $first->appendChild($strong);
        $row->appendChild($first);

        for ($i = 0; $i < 3; $i++) {
            $row->appendChild(new TableCell(TableCell::TYPE_DATA));
        }

        return $row;
    }

    private function buildHeader() : TableSection
    {
        $header = new TableSection(TableSection::TYPE_HEAD);
        $row = new TableRow();

        foreach (['Package', 'Installation', 'Downloads', 'Version'] as $label) {
            $cell = new TableCell(TableCell::TYPE_HEADER);
            $cell->appendChild(new Text($label));
            $row->appendChild($cell);
        }

        $header->appendChild($row);

        return $header;
    }

    /**
     * @param array{name: string, slug: string, type: string} $package
     */
    private function buildPackageRow(array $package) : TableRow
    {
        $row = new TableRow();

        $packageCell = new TableCell(TableCell::TYPE_DATA);
        $packageCell->appendChild(new Text($package['name']));
        $row->appendChild($packageCell);

        $installCell = new TableCell(TableCell::TYPE_DATA);
        $installLink = new Link('/documentation/installation/packages/' . $package['slug'] . '.md');
        $installLink->appendChild(new Text('Install'));
        $installCell->appendChild($installLink);
        $row->appendChild($installCell);

        $row->appendChild($this->buildBadgeCell(
            badgeUrl: 'https://poser.pugx.org/' . $package['name'] . '/downloads',
            badgeAlt: 'Total Downloads',
            packagistUrl: 'https://packagist.org/packages/' . $package['name'],
        ));
        $row->appendChild($this->buildBadgeCell(
            badgeUrl: 'https://poser.pugx.org/' . $package['name'] . '/v/stable',
            badgeAlt: 'Latest Stable Version',
            packagistUrl: 'https://packagist.org/packages/' . $package['name'],
        ));

        return $row;
    }

    private function buildTable() : Table
    {
        $table = new Table();

        $table->appendChild($this->buildHeader());
        $table->appendChild($this->buildBody());

        return $table;
    }

    /**
     * @return list<array{name: string, slug: string, type: string}>
     */
    private function packages() : array
    {
        if ($this->packagesCache !== null) {
            return $this->packagesCache;
        }

        if (!\is_file($this->manifestPath)) {
            throw new \RuntimeException(\sprintf('Flow manifest not found at "%s".', $this->manifestPath));
        }

        $raw = \file_get_contents($this->manifestPath);

        if ($raw === false) {
            throw new \RuntimeException(\sprintf('Failed to read Flow manifest at "%s".', $this->manifestPath));
        }

        /** @var array{packages?: list<array{name: string, path: string, type: string}>} $decoded */
        $decoded = \json_decode($raw, true, flags: \JSON_THROW_ON_ERROR);

        $packages = [];

        foreach ($decoded['packages'] ?? [] as $entry) {
            if (!\array_key_exists($entry['type'], self::TYPE_GROUPS)) {
                continue;
            }

            $packages[] = [
                'name' => $entry['name'],
                'slug' => self::slug($entry['name']),
                'type' => $entry['type'],
            ];
        }

        return $this->packagesCache = $packages;
    }

    private static function slug(string $composerName) : string
    {
        $slash = \strrpos($composerName, '/');

        return $slash === false ? $composerName : \substr($composerName, $slash + 1);
    }
}
