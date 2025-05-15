<?php

declare(strict_types=1);

namespace Flow\Website\EventListener;

use Flow\Website\Blog\Posts;
use Flow\Website\Service\Documentation\{DSLDefinitions, Pages};
use Flow\Website\Service\Examples;
use Presta\SitemapBundle\Event\SitemapPopulateEvent;
use Presta\SitemapBundle\Sitemap\Url\UrlConcrete;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Routing\Generator\UrlGeneratorInterface;

final class SitemapSubscriber implements EventSubscriberInterface
{
    public function __construct(
        private readonly Examples $examples,
        private readonly Posts $posts,
        private readonly DSLDefinitions $dslDefinitions,
        private readonly Pages $pages,
    ) {
    }

    public static function getSubscribedEvents()
    {
        return [
            SitemapPopulateEvent::class => 'populate',
        ];
    }

    public function populate(SitemapPopulateEvent $event) : void
    {
        $this->populateExamples($event);
        $this->populateBlogPosts($event);
        $this->populateDocumentation($event);
    }

    private function populateBlogPosts(SitemapPopulateEvent $event) : void
    {
        $posts = $this->posts->all();

        foreach ($posts as $post) {
            $event->getUrlContainer()->addUrl(
                new UrlConcrete(
                    $event->getUrlGenerator()->generate(
                        'blog_post',
                        ['date' => $post->date->format('Y-m-d'), 'slug' => $post->slug],
                        UrlGeneratorInterface::ABSOLUTE_URL
                    ),
                    changefreq: 'weekly'
                ),
                'blog'
            );
        }
    }

    private function populateDocumentation(SitemapPopulateEvent $event) : void
    {
        $modules = $this->dslDefinitions->modules();

        foreach ($modules as $module) {
            $event->getUrlContainer()->addUrl(
                new UrlConcrete(
                    $event->getUrlGenerator()->generate(
                        'documentation_dsl',
                        ['module' => \mb_strtolower($module->name)],
                        UrlGeneratorInterface::ABSOLUTE_URL
                    ),
                    changefreq: 'weekly'
                ),
                'documentation'
            );

            foreach ($this->dslDefinitions->fromModule($module)->all() as $definition) {
                $event->getUrlContainer()->addUrl(
                    new UrlConcrete(
                        $event->getUrlGenerator()->generate(
                            'documentation_dsl_function',
                            ['module' => \mb_strtolower($module->name), 'function' => $definition->slug()],
                            UrlGeneratorInterface::ABSOLUTE_URL
                        ),
                        changefreq: 'weekly'
                    ),
                    'documentation'
                );
            }
        }

        foreach ($this->pages->all() as $page) {
            $event->getUrlContainer()->addUrl(
                new UrlConcrete(
                    $event->getUrlGenerator()->generate(
                        'documentation_page',
                        ['path' => $page->path],
                        UrlGeneratorInterface::ABSOLUTE_URL
                    ),
                    changefreq: 'weekly'
                ),
                'documentation'
            );
        }
    }

    private function populateExamples(SitemapPopulateEvent $event) : void
    {
        $topics = $this->examples->topics();

        foreach ($topics as $topic) {
            $event->getUrlContainer()->addUrl(
                new UrlConcrete(
                    $event->getUrlGenerator()->generate(
                        'topic',
                        ['topic' => $topic],
                        UrlGeneratorInterface::ABSOLUTE_URL
                    ),
                    changefreq: 'weekly'
                ),
                'examples'
            );

            $examples = $this->examples->examples($topic);

            foreach ($examples as $example) {
                $event->getUrlContainer()->addUrl(
                    new UrlConcrete(
                        $event->getUrlGenerator()->generate(
                            'example',
                            ['topic' => $topic, 'example' => $example],
                            UrlGeneratorInterface::ABSOLUTE_URL
                        ),
                        changefreq: 'weekly'
                    ),
                    'examples'
                );
            }
        }
    }
}
