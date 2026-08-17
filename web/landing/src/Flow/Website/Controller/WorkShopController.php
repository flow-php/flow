<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

use function file_get_contents;
use function Flow\Types\DSL\type_string;

final class WorkShopController extends AbstractController
{
    /**
     * @param array<string, string> $checkoutLinks
     */
    public function __construct(
        private readonly array $checkoutLinks,
    ) {}

    #[Route('/work-shop', name: 'work_shop', options: ['sitemap' => false])]
    public function index(): Response
    {
        return $this->render('work-shop/index.html.twig');
    }

    #[Route('/work-shop/blueprints/symfony-backoffice', name: 'work_shop_blueprint_symfony', options: [
        'sitemap' => false,
    ])]
    public function blueprintSymfony(): Response
    {
        return $this->render('work-shop/blueprints/symfony-backoffice/index.html.twig', [
            'listing_checkout_url' => $this->checkoutLinks['symfony_backoffice'] ?? null,
        ]);
    }

    #[Route('/work-shop/blueprints/how-it-works', name: 'work_shop_blueprint_how_it_works', options: [
        'sitemap' => false,
    ])]
    public function blueprintHowItWorks(): Response
    {
        return $this->render('work-shop/blueprints/how-it-works/index.html.twig', [
            'listing_checkout_url' => $this->checkoutLinks['symfony_backoffice'] ?? null,
        ]);
    }

    #[Route('/work-shop/ai/claude-skills', name: 'work_shop_ai_claude_skills', options: [
        'sitemap' => false,
    ])]
    public function aiClaudeSkills(): Response
    {
        return $this->render('work-shop/ai/claude-skills/index.html.twig', [
            'listing_checkout_url' => $this->checkoutLinks['claude_skills'] ?? null,
        ]);
    }

    #[Route('/work-shop/ai/how-it-works', name: 'work_shop_ai_how_it_works', options: [
        'sitemap' => false,
    ])]
    public function aiHowItWorks(): Response
    {
        return $this->render('work-shop/ai/how-it-works/index.html.twig');
    }

    #[Route('/work-shop/sponsoring/1-month', name: 'work_shop_sponsoring_1m', options: [
        'sitemap' => false,
    ])]
    public function sponsoring1Month(): Response
    {
        return $this->render('work-shop/sponsoring/1-month/index.html.twig', [
            'listing_checkout_url' => $this->checkoutLinks['sponsoring_1m'] ?? null,
        ]);
    }

    #[Route('/work-shop/sponsoring/6-months', name: 'work_shop_sponsoring_6m', options: [
        'sitemap' => false,
    ])]
    public function sponsoring6Months(): Response
    {
        return $this->render('work-shop/sponsoring/6-months/index.html.twig', [
            'listing_checkout_url' => $this->checkoutLinks['sponsoring_6m'] ?? null,
        ]);
    }

    #[Route('/work-shop/sponsoring/12-months', name: 'work_shop_sponsoring_12m', options: [
        'sitemap' => false,
    ])]
    public function sponsoring12Months(): Response
    {
        return $this->render('work-shop/sponsoring/12-months/index.html.twig', [
            'listing_checkout_url' => $this->checkoutLinks['sponsoring_12m'] ?? null,
        ]);
    }

    #[Route('/work-shop/success', name: 'work_shop_success', options: ['sitemap' => false])]
    public function success(): Response
    {
        return $this->render('work-shop/success/index.html.twig');
    }

    #[Route('/work-shop/consulting', name: 'work_shop_consulting', options: ['sitemap' => false])]
    public function consulting(): Response
    {
        return $this->render('work-shop/consulting/index.html.twig');
    }

    #[Route('/work-shop/terms-of-sales', name: 'work_shop_terms_of_sales', options: ['sitemap' => false])]
    public function termsOfSales(): Response
    {
        return $this->render('work-shop/terms-of-sales/index.html.twig', [
            'terms_markdown' => file_get_contents(
                type_string()->assert($this->getParameter('kernel.project_dir'))
                    . '/content/work-shop/terms-of-sales.md',
            ),
        ]);
    }

    #[Route('/work-shop/privacy-policy', name: 'work_shop_privacy_policy', options: ['sitemap' => false])]
    public function privacyPolicy(): Response
    {
        return $this->render('work-shop/privacy-policy/index.html.twig', [
            'privacy_markdown' => file_get_contents(
                type_string()->assert($this->getParameter('kernel.project_dir'))
                    . '/content/work-shop/privacy-policy.md',
            ),
        ]);
    }
}
