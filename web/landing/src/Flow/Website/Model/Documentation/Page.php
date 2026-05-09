<?php

declare(strict_types=1);

namespace Flow\Website\Model\Documentation;

use League\CommonMark\Extension\FrontMatter\Data\SymfonyYamlFrontMatterParser;
use League\CommonMark\Extension\FrontMatter\FrontMatterParser;

final readonly class Page
{
    public function __construct(
        public string $path,
        public string $content,
    ) {
    }

    public function description() : ?string
    {
        $frontMatterParser = new FrontMatterParser(new SymfonyYamlFrontMatterParser());
        $result = $frontMatterParser->parse($this->content);

        return $result->getFrontMatter()['seo_description'] ?? 'Documentation';
    }

    public function editOnGitHubUrl() : string
    {
        return 'https://github.com/flow-php/flow/edit/1.x/documentation/' . ltrim($this->path, '/');
    }

    public function package() : ?string
    {
        $frontMatterParser = new FrontMatterParser(new SymfonyYamlFrontMatterParser());
        $result = $frontMatterParser->parse($this->content);
        $package = $result->getFrontMatter()['package'] ?? null;

        return is_string($package) ? $package : null;
    }

    public function title() : ?string
    {
        $frontMatterParser = new FrontMatterParser(new SymfonyYamlFrontMatterParser());
        $result = $frontMatterParser->parse($this->content);

        return $result->getFrontMatter()['seo_title'] ?? 'Documentation';
    }
}
