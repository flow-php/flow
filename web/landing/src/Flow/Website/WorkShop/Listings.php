<?php

declare(strict_types=1);

namespace Flow\Website\WorkShop;

use InvalidArgumentException;

use function array_map;

/**
 * @import-type CategoryShape from Category
 */
final class Listings
{
    /** @var list<CategoryShape> */
    private array $categories = [
        [
            'id' => 'blueprints',
            'heading' => 'Blueprints',
            'subtitle' => 'Working applications you can clone, run and pull apart. Read how the pieces fit together, then build your own on top.',
            'listings' => [
                [
                    'name' => 'Symfony Backoffice Blueprint',
                    'slug' => 'symfony-backoffice',
                    'blurb' => 'A working Symfony back office built on Flow PHP. One schema per model drives the database, the API and the imports, and every request is traced from the browser down to the SQL query.',
                    'features' => [
                        'Schema drives the database, API and imports',
                        'Streaming CSV, JSON and XML exports',
                        'OpenTelemetry across browser and backend',
                    ],
                    'available' => true,
                    'price' => '$59',
                    'price_note' => null,
                    'badge' => null,
                    'highlight' => false,
                    'route' => 'work_shop_blueprint_symfony',
                    'image' => 'images/work-shop/symfony-backoffice-app.png',
                ],
                [
                    'name' => 'More Blueprints',
                    'slug' => 'more-blueprints',
                    'blurb' => 'The Symfony back office is the first one. More are being built, same standards, different frameworks and different problems.',
                    'features' => [
                        'Fully tested and statically analysed',
                        'Something you want to see? Let us know',
                    ],
                    'available' => false,
                    'price' => null,
                    'price_note' => null,
                    'badge' => null,
                    'highlight' => false,
                    'route' => null,
                    'image' => null,
                ],
            ],
        ],
        [
            'id' => 'ai',
            'heading' => 'AI',
            'subtitle' => 'Claude Code plugins that make your agent work the way this codebase is built: planned tasks, gated reviews, and PHP quality rules that load themselves.',
            'listings' => [
                [
                    'name' => 'Claude Code Skills',
                    'slug' => 'claude-skills',
                    'blurb' => 'Two Claude Code plugins from one private marketplace: a task-driven engineering workflow with eleven gated skills, and PHP code-quality skills that auto-load whenever Claude touches PHP.',
                    'features' => [
                        'Plan, implement, clean up and review through gated skills',
                        'Findings survive adversarial verification before you see them',
                        'Type narrowing and testing rules that load themselves',
                    ],
                    'available' => true,
                    'price' => '$10',
                    'price_note' => null,
                    'badge' => null,
                    'highlight' => false,
                    'route' => 'work_shop_ai_claude_skills',
                    'image' => 'images/work-shop/claude-skills.svg',
                ],
            ],
        ],
        [
            'id' => 'sponsoring',
            'heading' => 'Sponsoring',
            'subtitle' => 'Flow PHP is MIT and free. Sponsoring funds the work that keeps it that way, and gets you into the sponsor-only channels on our Discord.',
            'listings' => [
                [
                    'name' => 'Sponsoring — 1 Month',
                    'slug' => 'sponsoring-1-month',
                    'blurb' => 'A small monthly contribution towards maintenance, releases and infrastructure. In exchange: the Sponsor role on our Discord — a custom username color and the exclusive channels where the roadmap happens.',
                    'features' => [
                        'Sponsor role with a custom username color',
                        'Access to exclusive sponsor-only channels',
                        'Renews monthly, cancel anytime',
                    ],
                    'available' => true,
                    'price' => '$5',
                    'price_note' => null,
                    'badge' => null,
                    'highlight' => false,
                    'route' => 'work_shop_sponsoring_1m',
                    'image' => 'images/work-shop/sponsoring-1m.svg',
                ],
                [
                    'name' => 'Sponsoring — 6 Months',
                    'slug' => 'sponsoring-6-months',
                    'blurb' => 'The same sponsorship in one payment every six months, 3% off the monthly price. The Sponsor role, a custom username color and the exclusive channels included.',
                    'features' => [
                        'Everything from the monthly sponsorship',
                        'Save 3% vs paying monthly',
                        'Renews every 6 months, cancel anytime',
                    ],
                    'available' => true,
                    'price' => '$29',
                    'price_note' => 'save 3%',
                    'badge' => 'Best value',
                    'highlight' => true,
                    'route' => 'work_shop_sponsoring_6m',
                    'image' => 'images/work-shop/sponsoring-6m.svg',
                ],
                [
                    'name' => 'Sponsoring — 12 Months',
                    'slug' => 'sponsoring-12-months',
                    'blurb' => 'A full year of sponsoring in one payment, 8% off the monthly price. The Sponsor role, a custom username color and the exclusive channels included.',
                    'features' => [
                        'Everything from the monthly sponsorship',
                        'Save 8% vs paying monthly',
                        'Renews every 12 months, cancel anytime',
                    ],
                    'available' => true,
                    'price' => '$55',
                    'price_note' => 'save 8%',
                    'badge' => null,
                    'highlight' => false,
                    'route' => 'work_shop_sponsoring_12m',
                    'image' => 'images/work-shop/sponsoring-12m.svg',
                ],
            ],
        ],
        [
            'id' => 'consulting',
            'heading' => 'Consulting',
            'subtitle' => 'Data engineering, architecture and observability work, from a two day workshop to a fractional architect on your team.',
            'listings' => [
                [
                    'name' => 'Consulting',
                    'slug' => 'consulting',
                    'blurb' => 'Sixteen years of data engineering, software architecture and observability work, from pipelines that hold up to APM bills that come down.',
                    'features' => [
                        'Data engineering, architecture, observability',
                        'Workshops, fractional architect, or hands-on build',
                        'Quoted per engagement, first call free',
                    ],
                    'available' => true,
                    'price' => null,
                    'price_note' => null,
                    'badge' => null,
                    'highlight' => false,
                    'route' => 'work_shop_consulting',
                    'image' => 'images/work-shop/consulting.jpg',
                ],
            ],
        ],
    ];

    /**
     * @return list<Category>
     */
    public function all(): array
    {
        return array_map(Category::fromArray(...), $this->categories);
    }

    public function findBySlug(string $slug): Listing
    {
        foreach ($this->all() as $category) {
            foreach ($category->listings as $listing) {
                if ($listing->slug === $slug) {
                    return $listing;
                }
            }
        }

        throw new InvalidArgumentException('Listing not found: ' . $slug);
    }
}
