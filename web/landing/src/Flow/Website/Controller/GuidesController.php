<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Model\Documentation\Page;
use Flow\Website\Service\Documentation\Pages;
use Flow\Website\SocialCard\Guides\CardGenerator;
use RuntimeException;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\DependencyInjection\Attribute\Autowire;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

use function array_filter;
use function array_keys;
use function array_slice;
use function ksort;
use function uksort;
use function usort;

final class GuidesController extends AbstractController
{
    private const array GROUP_POSITIONS = [
        'Reading files' => 1,
        'Writing files' => 2,
        'Importing data' => 3,
        'Search' => 4,
    ];

    public function __construct(
        #[Autowire(service: 'guides.pages')]
        private readonly Pages $guides,
    ) {}

    #[Route('/guides/{slug}', name: 'guides_guide')]
    public function guide(string $slug, CardGenerator $socialCards): Response
    {
        try {
            $guide = $this->guides->get($slug);
        } catch (RuntimeException) {
            throw $this->createNotFoundException();
        }

        $others = array_filter($this->guides->all(), static fn(Page $other): bool => $other->path !== $slug);

        usort(
            $others,
            static fn(Page $left, Page $right): int => (
                [$left->group() !== $guide->group(), $left->path] <=> [
                    $right->group() !== $guide->group(),
                    $right->path,
                ]
            ),
        );

        return $this->render('guides/guide.html.twig', [
            'guide' => $guide,
            'slug' => $slug,
            'social_image' => $socialCards->generate($guide, $slug),
            'related' => array_slice($others, offset: 0, length: 3),
        ]);
    }

    #[Route('/guides', name: 'guides', options: ['sitemap' => true])]
    public function index(): Response
    {
        $groups = [];
        $featured = [];

        foreach ($this->guides->all() as $guide) {
            $groups[$guide->group()][$guide->path] = $guide;

            if ($guide->card()['featured']) {
                $featured[$guide->path] = $guide;
            }
        }

        uksort(
            $groups,
            static fn(string $left, string $right): int => (
                [self::GROUP_POSITIONS[$left] ?? PHP_INT_MAX, $left] <=> [
                    self::GROUP_POSITIONS[$right] ?? PHP_INT_MAX,
                    $right,
                ]
            ),
        );

        foreach (array_keys($groups) as $group) {
            ksort($groups[$group]);
        }

        return $this->render('guides/index.html.twig', ['groups' => $groups, 'featured' => $featured]);
    }
}
