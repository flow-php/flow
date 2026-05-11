<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use Flow\Website\Service\Manifest\Manifest;
use League\CommonMark\CommonMarkConverter;
use League\CommonMark\Event\DocumentParsedEvent;
use League\CommonMark\Extension\CommonMark\Node\Block\FencedCode;
use League\CommonMark\Extension\CommonMark\Node\Inline\Link;
use League\CommonMark\Extension\ExternalLink\ExternalLinkExtension;
use League\CommonMark\Extension\FrontMatter\FrontMatterExtension;
use League\CommonMark\Extension\HeadingPermalink\HeadingPermalinkExtension;
use League\CommonMark\Extension\Mention\MentionExtension;
use League\CommonMark\Extension\Table\TableExtension;
use League\CommonMark\Extension\TableOfContents\Node\TableOfContents;
use League\CommonMark\Extension\TableOfContents\TableOfContentsExtension;
use Symfony\Component\Asset\Packages;
use Symfony\Component\DependencyInjection\ParameterBag\ContainerBagInterface;

final readonly class LeagueCommonMarkConverterFactory
{
    public function __construct(
        private ContainerBagInterface $parameters,
        private Packages $packages,
        private Manifest $manifest,
    ) {}

    public function __invoke(): CommonMarkConverter
    {
        $config = [
            'external_link' => [
                'open_in_new_window' => true,
                'noreferrer' => 'all',
            ],
            'mentions' => [
                'github_handle' => [
                    'prefix' => '@',
                    'pattern' => '[a-z\d](?:[a-z\d]|-(?=[a-z\d])){0,38}(?!\w)',
                    'generator' => 'https://github.com/%s',
                ],
                'github_issue' => [
                    'prefix' => '#',
                    'pattern' => '\d+',
                    'generator' => 'https://github.com/flow-php/flow/issues/%d',
                ],
            ],
            'heading_permalink' => [
                'html_class' => 'mr-2',
                'id_prefix' => '',
                'fragment_prefix' => '',
                'insert' => 'before',
                'title' => 'Permalink',
                'symbol' => '#',
            ],
            'table_of_contents' => [
                'html_class' => 'table-of-contents',
                'position' => 'placeholder',
                'placeholder' => '[TOC]',
                'style' => 'bullet',
                'min_heading_level' => 2,
                'max_heading_level' => 4,
                'normalize' => 'relative',
            ],
        ];

        $converter = new CommonMarkConverter($config);

        $converter
            ->getEnvironment()
            ->addExtension(new HeadingPermalinkExtension())
            ->addExtension(new TableOfContentsExtension())
            ->addExtension(new ExternalLinkExtension())
            ->addExtension(new FrontMatterExtension())
            ->addExtension(new MentionExtension())
            ->addExtension(new TableExtension())
            ->addRenderer(FencedCode::class, new MermaidCodeRenderer($this->packages), 100)
            ->addRenderer(FencedCode::class, new FlowCodeRenderer(), 0)
            ->addRenderer(Link::class, new FlowLinkRenderer(), 0)
            ->addRenderer(TableOfContents::class, new TableOfContentsRenderer(), 10)
            ->addEventListener(
                DocumentParsedEvent::class,
                new FlowVersionReplacer($this->parameters->get('flow_version')),
            )
            ->addEventListener(DocumentParsedEvent::class, new FlowManifestRenderer($this->manifest))
            ->addEventListener(DocumentParsedEvent::class, new FlowPackageNavRenderer($this->manifest))
            ->addEventListener(DocumentParsedEvent::class, new FlowDocLinkRenderer());

        return $converter;
    }
}
