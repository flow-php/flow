<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use Flow\Website\Service\Manifest\Manifest;
use League\CommonMark\Event\DocumentParsedEvent;
use League\CommonMark\Extension\CommonMark\Node\Inline\Image;
use League\CommonMark\Extension\CommonMark\Node\Inline\Link;
use League\CommonMark\Extension\CommonMark\Node\Inline\Strong;
use League\CommonMark\Extension\Table\Table;
use League\CommonMark\Extension\Table\TableCell;
use League\CommonMark\Extension\Table\TableRow;
use League\CommonMark\Extension\Table\TableSection;
use League\CommonMark\Node\Block\Paragraph;
use League\CommonMark\Node\Inline\Text;

use function array_key_exists;
use function array_keys;
use function strcmp;
use function strrpos;
use function substr;
use function usort;

final readonly class FlowManifestRenderer
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

    public function __construct(
        private Manifest $manifest,
    ) {}

    public function __invoke(DocumentParsedEvent $event): void
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

    private function buildBadgeCell(string $badgeUrl, string $badgeAlt, string $packagistUrl): TableCell
    {
        $cell = new TableCell(TableCell::TYPE_DATA);
        $link = new Link($packagistUrl);
        $link->appendChild(new Image($badgeUrl, $badgeAlt));
        $cell->appendChild($link);

        return $cell;
    }

    private function buildBody(): TableSection
    {
        $body = new TableSection(TableSection::TYPE_BODY);

        $grouped = [];

        foreach ($this->packages() as $package) {
            $grouped[$package['type']][] = $package;
        }

        foreach (array_keys(self::TYPE_GROUPS) as $type) {
            if (!array_key_exists($type, $grouped)) {
                continue;
            }

            $body->appendChild($this->buildGroupHeadingRow(self::TYPE_GROUPS[$type]['label']));

            $items = $grouped[$type];
            usort($items, static fn(array $a, array $b): int => strcmp($a['name'], $b['name']));

            foreach ($items as $package) {
                $body->appendChild($this->buildPackageRow($package));
            }
        }

        return $body;
    }

    private function buildGroupHeadingRow(string $label): TableRow
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

    private function buildHeader(): TableSection
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
    private function buildPackageRow(array $package): TableRow
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

    private function buildTable(): Table
    {
        $table = new Table();

        $table->appendChild($this->buildHeader());
        $table->appendChild($this->buildBody());

        return $table;
    }

    /**
     * @return list<array{name: string, slug: string, type: string}>
     */
    private function packages(): array
    {
        $packages = [];

        foreach ($this->manifest->all() as $entry) {
            $type = is_string($entry['type'] ?? null) ? $entry['type'] : null;
            $name = is_string($entry['name'] ?? null) ? $entry['name'] : null;

            if ($type === null || $name === null) {
                continue;
            }

            if (!array_key_exists($type, self::TYPE_GROUPS)) {
                continue;
            }

            $packages[] = [
                'name' => $name,
                'slug' => self::slug($name),
                'type' => $type,
            ];
        }

        return $packages;
    }

    private static function slug(string $composerName): string
    {
        $slash = strrpos($composerName, '/');

        return $slash === false ? $composerName : substr($composerName, $slash + 1);
    }
}
