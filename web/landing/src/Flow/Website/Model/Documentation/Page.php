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

    /**
     * @return array{problem: ?string, proof_label: ?string, proof_before: ?string, proof_after: ?string, featured: bool}
     */
    public function card(): array
    {
        $frontMatter = $this->frontMatter();

        return [
            'problem' => type_union(type_string(), type_null())->assert($frontMatter['problem'] ?? null),
            'proof_label' => type_union(type_string(), type_null())->assert($frontMatter['proof_label'] ?? null),
            'proof_before' => type_union(type_string(), type_null())->assert($frontMatter['proof_before'] ?? null),
            'proof_after' => type_union(type_string(), type_null())->assert($frontMatter['proof_after'] ?? null),
            'featured' => ($frontMatter['featured'] ?? false) === true,
        ];
    }

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

    public function group(): string
    {
        return type_string()->assert($this->frontMatter()['group'] ?? null);
    }

    public function package(): ?string
    {
        $frontMatter = $this->frontMatter();

        return type_union(type_string(), type_null())->assert($frontMatter['package'] ?? null);
    }

    public function published(): string
    {
        return type_string()->assert($this->frontMatter()['published'] ?? null);
    }

    public function title(): ?string
    {
        $frontMatter = $this->frontMatter();

        return type_union(type_string(), type_null())->assert($frontMatter['seo_title'] ?? null) ?? 'Documentation';
    }

    public function updated(): string
    {
        return type_string()->assert($this->frontMatter()['updated'] ?? null);
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
