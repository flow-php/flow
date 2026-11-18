<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Service\Documentation\Pages;
use RuntimeException;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\DependencyInjection\Attribute\Autowire;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

use function array_keys;
use function ksort;

final class GuidesController extends AbstractController
{
    public function __construct(
        #[Autowire(service: 'guides.pages')]
        private readonly Pages $guides,
    ) {}

    #[Route('/guides/{slug}', name: 'guides_guide')]
    public function guide(string $slug): Response
    {
        try {
            $guide = $this->guides->get($slug);
        } catch (RuntimeException) {
            throw $this->createNotFoundException();
        }

        return $this->render('guides/guide.html.twig', ['guide' => $guide, 'slug' => $slug]);
    }

    #[Route('/guides', name: 'guides', options: ['sitemap' => true])]
    public function index(): Response
    {
        $groups = [];

        foreach ($this->guides->all() as $guide) {
            $groups[$guide->group()][$guide->path] = $guide;
        }

        ksort($groups);

        foreach (array_keys($groups) as $group) {
            ksort($groups[$group]);
        }

        return $this->render('guides/index.html.twig', ['groups' => $groups]);
    }
}
