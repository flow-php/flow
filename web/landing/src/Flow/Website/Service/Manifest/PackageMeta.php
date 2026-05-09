<?php

declare(strict_types=1);

namespace Flow\Website\Service\Manifest;

final readonly class PackageMeta
{
    private const array ACRONYMS = [
        'csv' => 'CSV',
        'json' => 'JSON',
        'xml' => 'XML',
        'http' => 'HTTP',
        'psr3' => 'PSR3',
        'psr7' => 'PSR7',
        'psr18' => 'PSR18',
        'otlp' => 'OTLP',
        'dbal' => 'DBAL',
        'sdk' => 'SDK',
        'cli' => 'CLI',
        'postgresql' => 'PostgreSQL',
    ];

    private const array DSL_MODULE_COMPONENT = [
        'core' => 'Core',
        'csv' => 'CSV',
        'json' => 'JSON',
        'xml' => 'XML',
        'parquet' => 'Parquet',
        'avro' => 'Avro',
        'text' => 'Text',
        'http' => 'HTTP',
        'doctrine' => 'Doctrine',
        'elasticsearch' => 'Elasticsearch',
        'chartjs' => 'ChartJS',
        'meilisearch' => 'Meilisearch',
        'google-sheet' => 'Google Sheet',
        'logger' => 'Logger',
        'excel' => 'Excel',
    ];

    private const array NAME_OVERRIDE = [
        'flow-php/etl' => 'Core',
    ];

    private const array TYPE_LABEL = [
        'core' => 'Core',
        'cli' => 'CLI',
        'adapter' => 'Adapter',
        'bridge' => 'Bridge',
        'lib' => 'Library',
        'extension' => 'Extension',
    ];

    public function __construct(private Manifest $manifest)
    {
    }

    public function forDslModule(string $module) : ?string
    {
        return self::DSL_MODULE_COMPONENT[strtolower($module)] ?? null;
    }

    /**
     * @return null|array{type: string, component: string}
     */
    public function forPackage(string $packageName) : ?array
    {
        $entry = $this->manifest->byName($packageName);

        if ($entry === null) {
            return null;
        }

        $type = is_string($entry['type'] ?? null) ? $entry['type'] : null;

        if ($type === null || !isset(self::TYPE_LABEL[$type])) {
            return null;
        }

        return [
            'type' => self::TYPE_LABEL[$type],
            'component' => $this->componentLabel($packageName),
        ];
    }

    private function componentLabel(string $packageName) : string
    {
        if (isset(self::NAME_OVERRIDE[$packageName])) {
            return self::NAME_OVERRIDE[$packageName];
        }

        $slash = strrpos($packageName, '/');
        $slug = $slash === false ? $packageName : substr($packageName, $slash + 1);

        if (str_starts_with($slug, 'etl-adapter-')) {
            $slug = substr($slug, strlen('etl-adapter-'));
        }

        $words = explode('-', $slug);
        $labelled = [];

        foreach ($words as $word) {
            $labelled[] = self::ACRONYMS[$word] ?? ucfirst($word);
        }

        return implode(' ', $labelled);
    }
}
