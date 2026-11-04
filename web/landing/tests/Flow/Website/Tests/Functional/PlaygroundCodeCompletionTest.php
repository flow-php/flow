<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Flow\Website\Tests\Context\CodeCompletionContext;

use function str_repeat;

final class PlaygroundCodeCompletionTest extends EndToEndTestCase
{
    public function test_suggests_data_frame_methods_after_a_long_multiline_chain(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $rows = str_repeat("['id' => 1, 'name' => 'a long enough value to push the call far away'], ", 60);
        $this->setPlaygroundCode(
            $page,
            "<?php\n\nuse function Flow\\ETL\\DSL\\{data_frame, from_array};\n\ndata_frame()\n    ->read(from_array([{$rows}]))\n    ",
        );

        static::assertContains('write', (new CodeCompletionContext($page))->suggestionsAfterTyping('->wri'));
    }

    public function test_suggests_grouped_data_frame_methods_after_group_by(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode(
            $page,
            "<?php\n\nuse function Flow\\ETL\\DSL\\{data_frame, from_array};\n\ndata_frame()->read(from_array([]))->groupBy('id')",
        );

        static::assertSame(['pivot'], (new CodeCompletionContext($page))->suggestionsAfterTyping('->piv'));
    }

    public function test_suggests_scalar_function_chain_methods_after_any_chained_method(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nuse function Flow\\ETL\\DSL\\ref;\n\nref('a(')->upper()->isNull()");

        static::assertContains('and', (new CodeCompletionContext($page))->suggestionsAfterTyping('->an'));
    }

    public function test_suggests_methods_of_classes_returned_by_adapter_dsl(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode(
            $page,
            "<?php\n\nuse function Flow\\ETL\\Adapter\\CSV\\from_csv;\n\nfrom_csv('orders.csv')",
        );

        static::assertContains('withHeader', (new CodeCompletionContext($page))->suggestionsAfterTyping('->withHea'));
    }

    public function test_suggests_methods_of_an_assigned_variable(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode(
            $page,
            "<?php\n\nuse function Flow\\ETL\\DSL\\{data_frame, from_array};\n\n\$df = data_frame()->read(from_array([]));\n\n\$df",
        );

        static::assertContains('limit', (new CodeCompletionContext($page))->suggestionsAfterTyping('->lim'));
    }

    public function test_suggests_nothing_inside_a_string(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\n\$name = ");

        static::assertSame([], (new CodeCompletionContext($page))->suggestionsSettledAfterTyping("'data_fr"));
    }

    public function test_accepting_a_function_adds_it_to_the_matching_import_group(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nuse function Flow\\ETL\\DSL\\{data_frame, to_output};\n\n");
        $completion = new CodeCompletionContext($page);

        $completion->suggestionsAfterTyping('from_arra');
        $completion->accept();

        static::assertSame(
            "<?php\n\nuse function Flow\\ETL\\DSL\\{data_frame, from_array, to_output};\n\nfrom_array()",
            $this->getPlaygroundCode($page),
        );
    }

    public function test_accepting_a_function_appends_to_a_multiline_import_group(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode(
            $page,
            "<?php\n\nuse function Flow\\ETL\\DSL\\{\n    data_frame,\n    from_array,\n};\n\n",
        );
        $completion = new CodeCompletionContext($page);

        $completion->suggestionsAfterTyping('to_outp');
        $completion->accept();

        static::assertSame(
            "<?php\n\nuse function Flow\\ETL\\DSL\\{\n    data_frame,\n    from_array,\n    to_output,\n};\n\nto_output()",
            $this->getPlaygroundCode($page),
        );
    }

    public function test_accepting_a_function_adds_an_import_after_the_last_use_function(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nuse function Flow\\ETL\\Adapter\\CSV\\from_csv;\n\n");
        $completion = new CodeCompletionContext($page);

        $completion->suggestionsAfterTyping('data_fram');
        $completion->accept();

        static::assertSame(
            "<?php\n\nuse function Flow\\ETL\\Adapter\\CSV\\from_csv;\nuse function Flow\\ETL\\DSL\\data_frame;\n\ndata_frame()",
            $this->getPlaygroundCode($page),
        );
    }

    public function test_accepting_a_function_adds_the_first_import_after_declare(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\ndeclare(strict_types=1);\n\n");
        $completion = new CodeCompletionContext($page);

        $completion->suggestionsAfterTyping('data_fram');
        $completion->accept();

        static::assertSame(
            "<?php\n\ndeclare(strict_types=1);\n\nuse function Flow\\ETL\\DSL\\data_frame;\n\ndata_frame()",
            $this->getPlaygroundCode($page),
        );
    }

    public function test_accepting_an_already_imported_function_only_inserts_the_call(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nuse function Flow\\ETL\\DSL\\data_frame;\n\n");
        $completion = new CodeCompletionContext($page);

        $completion->suggestionsAfterTyping('data_fram');
        $completion->accept();

        static::assertSame(
            "<?php\n\nuse function Flow\\ETL\\DSL\\data_frame;\n\ndata_frame()",
            $this->getPlaygroundCode($page),
        );
    }

    public function test_accepting_a_function_whose_name_is_taken_inserts_its_qualified_name(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nuse function Flow\\PostgreSql\\DSL\\col;\n\n");
        $completion = new CodeCompletionContext($page);

        $completion->suggestionsAfterTyping('col');
        $completion->accept();

        static::assertSame(
            "<?php\n\nuse function Flow\\PostgreSql\\DSL\\col;\n\n\\Flow\\ETL\\DSL\\col()",
            $this->getPlaygroundCode($page),
        );
    }

    public function test_accepting_a_class_imports_it_before_function_imports(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode(
            $page,
            "<?php\n\nuse function Flow\\Types\\DSL\\type_instance_of;\n\ntype_instance_of(",
        );
        $completion = new CodeCompletionContext($page);

        static::assertSame('Report', $completion->suggestionsAfterTyping('Repor')[0]);
        $completion->accept();

        static::assertSame(
            "<?php\n\nuse Flow\\ETL\\Runtime\\Metrics\\Report;\nuse function Flow\\Types\\DSL\\type_instance_of;\n\ntype_instance_of(Report",
            $this->getPlaygroundCode($page),
        );
    }

    public function test_suggests_only_classes_after_new_and_imports_after_the_last_class_import(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nuse Flow\\ETL\\Runtime\\Metrics\\Report;\n\nnew ");
        $completion = new CodeCompletionContext($page);

        $suggestions = $completion->suggestionsAfterTyping('ConfigBuild');

        static::assertSame('ConfigBuilder', $suggestions[0]);
        static::assertNotContains('config_builder', $suggestions);
        $completion->accept();

        static::assertSame(
            "<?php\n\nuse Flow\\ETL\\Runtime\\Metrics\\Report;\nuse Flow\\ETL\\API\\Config\\ConfigBuilder;\n\nnew ConfigBuilder",
            $this->getPlaygroundCode($page),
        );
    }

    public function test_suggests_enum_cases_after_double_colon(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nuse Flow\\ETL\\IO\\Filesystem\\SaveMode;\n\nSaveMode");
        $completion = new CodeCompletionContext($page);

        static::assertContains('Overwrite', $completion->suggestionsAfterTyping('::Over'));
    }

    public function test_suggests_class_keyword_after_double_colon(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nuse Flow\\ETL\\Runtime\\Metrics\\Report;\n\nReport");

        static::assertSame(['class'], (new CodeCompletionContext($page))->suggestionsAfterTyping('::cla'));
    }

    public function test_suggests_methods_of_a_static_method_result(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nuse Flow\\ETL\\Values\\Schema;\n\nSchema::fromArray([])");

        static::assertContains('addMetadata', (new CodeCompletionContext($page))->suggestionsAfterTyping('->addMeta'));
    }

    public function test_tab_accepts_the_selected_suggestion(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nuse function Flow\\ETL\\DSL\\data_frame;\n\n");
        $completion = new CodeCompletionContext($page);

        $completion->suggestionsAfterTyping('data_fram');
        $completion->accept('Tab');

        static::assertSame(
            "<?php\n\nuse function Flow\\ETL\\DSL\\data_frame;\n\ndata_frame()",
            $this->getPlaygroundCode($page),
        );
    }

    public function test_tab_indents_when_no_suggestion_is_open(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\n");
        $completion = new CodeCompletionContext($page);

        $completion->type('');
        $completion->accept('Tab');

        static::assertMatchesRegularExpression('/^<\?php\n\n[ \t]+$/', $this->getPlaygroundCode($page));
    }

    public function test_suggests_classes_used_only_as_parameter_types(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\n\$plan->toString(format: ");

        static::assertContains('Format', (new CodeCompletionContext($page))->suggestionsAfterTyping('Forma'));
    }

    public function test_hover_shows_function_signature_and_documentation(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode(
            $page,
            "<?php\n\nuse function Flow\\ETL\\DSL\\to_output;\n\nto_output(truncate: false);",
        );

        $tooltip = (new CodeCompletionContext($page))->hoverTooltipOver('to_output', 1);

        static::assertStringContainsString('to_output(', $tooltip);
        static::assertStringContainsString('$truncate', $tooltip);
        static::assertStringContainsString('Documentation', $tooltip);
        static::assertSame(
            'to_output',
            $page->evaluate('() => document.querySelector(".cm-tooltip-hover .token.function")?.textContent'),
        );
    }

    public function test_hover_shows_resolved_method_of_a_chain(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode(
            $page,
            "<?php\n\nuse function Flow\\ETL\\DSL\\{data_frame, from_array};\n\ndata_frame()->read(from_array([]))->limit(10);",
        );

        $tooltip = (new CodeCompletionContext($page))->hoverTooltipOver('limit');

        static::assertStringContainsString('Flow\\ETL\\API\\DataFrame', $tooltip);
        static::assertStringContainsString('limit(', $tooltip);
        static::assertSame(
            '/documentation/api/core/classes/Flow-ETL-API-DataFrame.html#method_limit',
            $page->evaluate('() => document.querySelector(".cm-tooltip-hover a").getAttribute("href")'),
        );
    }

    public function test_hover_shows_the_type_of_an_assigned_variable(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode(
            $page,
            "<?php\n\nuse function Flow\\ETL\\DSL\\data_frame;\n\n\$flow = data_frame();\n\$flow->read(1);",
        );

        static::assertSame('$flow: Flow\\ETL\\Flow', (new CodeCompletionContext($page))->hoverTooltipOver('$flow', 1));
    }

    public function test_signature_hint_highlights_the_current_parameter(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nuse function Flow\\ETL\\DSL\\to_output;\n\n");
        $completion = new CodeCompletionContext($page);

        $completion->type('to_output(false, ');
        $hint = $completion->signatureHint();

        static::assertStringStartsWith('to_output(', $hint['text']);
        static::assertStringContainsString('$output', $hint['active']);
    }

    public function test_signature_hint_follows_a_named_argument(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nuse function Flow\\ETL\\DSL\\to_output;\n\n");
        $completion = new CodeCompletionContext($page);

        $completion->type('to_output(output: ');

        static::assertStringContainsString('$output', $completion->signatureHint()['active']);
    }

    public function test_suggests_named_arguments_that_are_not_used_yet(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode(
            $page,
            "<?php\n\nuse function Flow\\ETL\\DSL\\to_output;\n\nto_output(truncate: false, ",
        );
        $suggestions = (new CodeCompletionContext($page))->suggestionsAfterTyping('o');

        static::assertSame('output:', $suggestions[0]);
        static::assertNotContains('truncate:', $suggestions);
    }

    public function test_suggests_variables_with_their_types(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode(
            $page,
            "<?php\n\nuse function Flow\\ETL\\DSL\\data_frame;\n\n\$flow = data_frame();\n\$fallback = 1;\n",
        );
        $completion = new CodeCompletionContext($page);

        static::assertEqualsCanonicalizing(['$flow', '$fallback'], $completion->suggestionsAfterTyping('$f'));
        static::assertSame(
            ['Flow'],
            $page->evaluate(
                '() => [...document.querySelectorAll(".cm-tooltip-autocomplete .cm-completionDetail")].map(detail => detail.textContent).filter(Boolean)',
            ),
        );
    }

    public function test_modifier_click_opens_function_documentation(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode(
            $page,
            "<?php\n\nuse function Flow\\ETL\\DSL\\to_output;\n\nto_output(truncate: false);",
        );

        static::assertSame('/documentation/dsl/core/to-output#dsl-function', (new CodeCompletionContext(
            $page,
        ))->documentationOpenedByModifierClickOn('to_output', 1));
    }

    public function test_accepting_a_php_function_adds_no_import(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\n");
        $completion = new CodeCompletionContext($page);

        static::assertContains('array_map', $completion->suggestionsAfterTyping('array_ma'));

        $completion->accept();

        static::assertSame("<?php\n\narray_map()", $this->getPlaygroundCode($page));
    }

    public function test_accepting_a_php_class_in_the_global_namespace_adds_no_import(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nnew ");
        $completion = new CodeCompletionContext($page);

        static::assertSame(['DateTimeImmutable'], $completion->suggestionsAfterTyping('DateTimeImmu'));

        $completion->accept();

        static::assertSame("<?php\n\nnew DateTimeImmutable", $this->getPlaygroundCode($page));
    }

    public function test_accepting_a_php_class_in_a_namespace_imports_it(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\nnamespace App;\n\nnew ");
        $completion = new CodeCompletionContext($page);

        $completion->suggestionsAfterTyping('DateTimeImmu');
        $completion->accept();

        static::assertSame(
            "<?php\n\nnamespace App;\n\nuse DateTimeImmutable;\n\nnew DateTimeImmutable",
            $this->getPlaygroundCode($page),
        );
    }

    public function test_suggests_php_class_methods_after_a_chained_call(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\n(new DateTimeImmutable())->modify('+1 day')");

        static::assertContains('format', (new CodeCompletionContext($page))->suggestionsAfterTyping('->form'));
    }

    public function test_modifier_click_on_a_php_function_opens_the_php_manual(): void
    {
        $page = $this->pageOf($this->openPlayground('/playground'));
        $this->setPlaygroundCode($page, "<?php\n\narray_map(null, [1]);");

        static::assertSame(
            'https://www.php.net/manual/en/function.array-map.php',
            (new CodeCompletionContext($page))->documentationOpenedByModifierClickOn('array_map'),
        );
    }
}
