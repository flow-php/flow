<?php declare(strict_types=1);

use PhpCsFixer\Config;
use PhpCsFixer\Finder;

$finder = Finder::create()
    ->files()
    ->in([
        __DIR__ . '/src/core/**/src',
        __DIR__ . '/src/core/**/tests',
        __DIR__ . '/src/adapter/**/src',
        __DIR__ . '/src/adapter/**/tests',
        __DIR__ . '/src/lib/**/src',
        __DIR__ . '/src/lib/**/tests',
        __DIR__ . '/examples',
    ]);

if (!\file_exists(__DIR__ . '/var')) {
    \mkdir(__DIR__ . '/var');
}

if (!\file_exists(__DIR__ . '/var/cs-fixer')) {
    \mkdir(__DIR__ . '/var/cs-fixer');
}

return (new Config())
    ->setRiskyAllowed(true)
    ->setCacheFile(__DIR__ . '/var/cs-fixer/php_cs.cache')
    ->setRules([
        '@Symfony' => true,
        '@Symfony:risky' => true,
        'blank_line_after_opening_tag' => false,
        'blank_line_before_statement' => [
            'statements' => [
                'break',
                'continue',
                'declare',
                'default',
                'do',
                'exit',
                'for',
                'foreach',
                'goto',
                'if',
                'include',
                'include_once',
                'require',
                'require_once',
                'return',
                'switch',
                'throw',
                'try',
                'while',
            ],
        ],
        'blank_line_between_import_groups' => false,
        'blank_lines_before_namespace' => false,
        'class_attributes_separation' => ['elements' => ['const' => 'one', 'method' => 'one', 'property' => 'one']],
        'combine_consecutive_issets' => true,
        'combine_consecutive_unsets' => true,
        'concat_space' => ['spacing' => 'one'],
        'declare_strict_types' => true,
        'explicit_indirect_variable' => true,
        'explicit_string_variable' => true,
        'fopen_flags' => true,
        'heredoc_to_nowdoc' => true,
        'increment_style' => ['style' => 'post'],
        'linebreak_after_opening_tag' => false,
        'method_argument_space' => ['on_multiline' => 'ensure_fully_multiline'],
        'modernize_types_casting' => false,
        'multiline_comment_opening_closing' => true,
        'multiline_whitespace_before_semicolons' => true,
        'native_constant_invocation' => false,
        'native_function_invocation' => ['include' => ['@all']], // todo
        'new_with_parentheses' => false,
        'nullable_type_declaration_for_default_null_value' => true,
        'no_extra_blank_lines' => true, // todo?
        'no_mixed_echo_print' => ['use' => 'print'],
        'no_superfluous_elseif' => true,
        'no_superfluous_phpdoc_tags' => false,
        'no_unneeded_control_parentheses' => true, // todo?
        'no_unreachable_default_argument_value' => true,
        'no_unset_on_property' => true,
        'no_useless_else' => true,
        'no_useless_return' => true,
        'ordered_class_elements' => [
            'order' => [
                'use_trait',
                'constant_public',
                'constant_protected',
                'constant_private',
                'case',
                'property_public_static',
                'property_protected_static',
                'property_private_static',
                'property_public',
                'property_protected',
                'property_private',
                'construct',
                'method_public_static',
                'destruct',
                'magic',
                'phpunit',
                'method_public',
                'method_protected',
                'method_private',
                'method_protected_static',
                'method_private_static',
            ],
            'sort_algorithm' => 'alpha'
        ],
        'ordered_imports' => [
            'imports_order' => [
                'const',
                'function',
                'class',
            ],
            'sort_algorithm' => 'alpha',
        ],
        'ordered_interfaces' => [
            'direction' => 'ascend',
            'order' => 'alpha',
        ],
        'phpdoc_align' => ['align' => 'left'],
        'phpdoc_no_empty_return' => true,
        'phpdoc_order' => true,
        'phpdoc_to_comment' => false,
        'phpdoc_types_order' => true,
        'php_unit_method_casing' => ['case' => 'snake_case'],
        'protected_to_private' => true,
        'return_assignment' => false,
        'return_type_declaration' => ['space_before' => 'one'],
        'self_static_accessor' => true,
        'strict_param' => true,
        'ternary_to_null_coalescing' => true,
        'yoda_style' => false,
        'void_return' => true,
    ])
    ->setFinder($finder);
