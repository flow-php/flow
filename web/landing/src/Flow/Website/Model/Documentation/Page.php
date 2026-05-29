<?php

declare(strict_types=1);

namespace Flow\Website\Model\Documentation;

use League\CommonMark\Extension\FrontMatter\Data\SymfonyYamlFrontMatterParser;
use League\CommonMark\Extension\FrontMatter\FrontMatterParser;

use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final readonly class Page
{
    public function __construct(
        public string $path,
        public string $content,
    ) {}

    public function description(): ?string
    {
        $frontMatter = $this->frontMatter();

        return (
            type_union(type_string(), type_null())->assert($frontMatter['seo_description'] ?? null) ?? 'Documentation'
        );
    }

    public function editOnGitHubUrl(): string
    {
        return 'https://github.com/flow-php/flow/edit/1.x/documentation/' . ltrim($this->path, '/');
    }

    public function package(): ?string
    {
        $frontMatter = $this->frontMatter();

        return type_union(type_string(), type_null())->assert($frontMatter['package'] ?? null);
    }

    public function title(): ?string
    {
        $frontMatter = $this->frontMatter();

        return type_union(type_string(), type_null())->assert($frontMatter['seo_title'] ?? null) ?? 'Documentation';
    }

    /**
     * @return array<string, mixed>
     */
    private function frontMatter(): array
    {
        $frontMatter = type_union(type_map(type_string(), type_mixed()), type_null())->assert(
            (new FrontMatterParser(new SymfonyYamlFrontMatterParser()))
                ->parse($this->content)
                ->getFrontMatter(),
        );

        return $frontMatter ?? [];
    }
}
