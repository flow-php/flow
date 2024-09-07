<?php declare(strict_types=1);

return [
    Symfony\Bundle\FrameworkBundle\FrameworkBundle::class => ['all' => true],
    Symfony\Bundle\TwigBundle\TwigBundle::class => ['all' => true],
    Symfony\Bundle\WebProfilerBundle\WebProfilerBundle::class => ['dev' => true, 'test' => true],
    Symfonycasts\TailwindBundle\SymfonycastsTailwindBundle::class => ['all' => true],
    NorbertTech\StaticContentGeneratorBundle\StaticContentGeneratorBundle::class => ['all' => true],
    Symfony\UX\StimulusBundle\StimulusBundle::class => ['all' => true],
    Symfony\Bundle\MonologBundle\MonologBundle::class => ['all' => true],
    \Twig\Extra\TwigExtraBundle\TwigExtraBundle::class => ['all' => true],
    \Presta\SitemapBundle\PrestaSitemapBundle::class => ['all' => true],
];
