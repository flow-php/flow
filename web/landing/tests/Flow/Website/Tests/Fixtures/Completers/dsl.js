/**
 * CodeMirror Completer for Flow PHP DSL Functions
 *
 * Total functions: 699
 *
 * This completer provides autocompletion for all Flow PHP DSL functions:
 * - Extractors (flow-extractors)
 * - Loaders (flow-loaders)
 * - Transformers (flow-transformers)
 * - Scalar Functions (flow-scalar-functions)
 * - Aggregating Functions (flow-aggregating-functions)
 * - Window Functions (flow-window-functions)
 * - And more...
 */

import { CompletionContext, snippet } from "@codemirror/autocomplete"

// All DSL functions
const dslFunctions = [
                {
        label: "add_row_index",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">add_row_index</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$column</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;index&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StartFrom</span> <span class=\"fn-param\">$startFrom</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Transformation\\AddRowIndex\\StartFrom::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AddRowIndex</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\add_row_index(" + "$" + "{" + "1:column" + "}" + ", " + "$" + "{" + "2:startFrom" + "}" + ")"),
        boost: 10
    },                {
        label: "agg",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">agg</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$args</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$distinct</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AggregateCall</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an aggregate function call (COUNT, SUM, AVG, etc.).<br>@param string $name Aggregate function name<br>@param list<Expression> $args Function arguments<br>@param bool $distinct Use DISTINCT modifier
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\agg(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:args" + "}" + ", " + "$" + "{" + "3:distinct" + "}" + ")"),
        boost: 10
    },                {
        label: "agg_avg",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">agg_avg</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$distinct</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AggregateCall</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create AVG aggregate.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\agg_avg(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:distinct" + "}" + ")"),
        boost: 10
    },                {
        label: "agg_count",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">agg_count</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$distinct</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AggregateCall</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create COUNT(*) aggregate.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\agg_count(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:distinct" + "}" + ")"),
        boost: 10
    },                {
        label: "agg_max",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">agg_max</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AggregateCall</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create MAX aggregate.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\agg_max(" + "$" + "{" + "1:expr" + "}" + ")"),
        boost: 10
    },                {
        label: "agg_min",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">agg_min</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AggregateCall</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create MIN aggregate.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\agg_min(" + "$" + "{" + "1:expr" + "}" + ")"),
        boost: 10
    },                {
        label: "agg_sum",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">agg_sum</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$distinct</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AggregateCall</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create SUM aggregate.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\agg_sum(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:distinct" + "}" + ")"),
        boost: 10
    },                {
        label: "all",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$functions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">All</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\all(" + "$" + "{" + "1:functions" + "}" + ")"),
        boost: 10
    },                {
        label: "all_sub_select",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">all_sub_select</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ComparisonOperator</span> <span class=\"fn-param\">$operator</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SelectFinalStep</span> <span class=\"fn-param\">$subquery</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">All</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an ALL condition.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\all_sub_select(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:operator" + "}" + ", " + "$" + "{" + "3:subquery" + "}" + ")"),
        boost: 10
    },                {
        label: "alter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">alter</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AlterFactory</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a factory for building ALTER statements.<br>Provides a unified entry point for all ALTER operations:<br>- alter()->table() - ALTER TABLE<br>- alter()->index() - ALTER INDEX<br>- alter()->view() - ALTER VIEW<br>- alter()->materializedView() - ALTER MATERIALIZED VIEW<br>- alter()->sequence() - ALTER SEQUENCE<br>- alter()->schema() - ALTER SCHEMA<br>- alter()->role() - ALTER ROLE<br>- alter()->function() - ALTER FUNCTION<br>- alter()->procedure() - ALTER PROCEDURE<br>- alter()->trigger() - ALTER TRIGGER<br>- alter()->extension() - ALTER EXTENSION<br>- alter()->enumType() - ALTER TYPE (enum)<br>- alter()->domain() - ALTER DOMAIN<br>Rename operations are also under alter():<br>- alter()->index(\'old\')->renameTo(\'new\')<br>- alter()->view(\'old\')->renameTo(\'new\')<br>- alter()->schema(\'old\')->renameTo(\'new\')<br>- alter()->role(\'old\')->renameTo(\'new\')<br>- alter()->trigger(\'old\')->on(\'table\')->renameTo(\'new\')<br>Example: alter()->table(\'users\')->addColumn(col_def(\'email\', data_type_text()))<br>Example: alter()->sequence(\'user_id_seq\')->restart(1000)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\alter()"),
        boost: 10
    },                {
        label: "always_off_exemplar_filter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">always_off_exemplar_filter</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AlwaysOffExemplarFilter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an AlwaysOffExemplarFilter.<br>Never records exemplars. Use this filter to disable exemplar collection<br>entirely for performance optimization.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\always_off_exemplar_filter()"),
        boost: 10
    },                {
        label: "always_on_exemplar_filter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">always_on_exemplar_filter</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AlwaysOnExemplarFilter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an AlwaysOnExemplarFilter.<br>Records exemplars whenever a span context is present.<br>Use this filter for debugging or when complete trace context is important.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\always_on_exemplar_filter()"),
        boost: 10
    },                {
        label: "analyze",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">analyze</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Analyze</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\analyze()"),
        boost: 10
    },                {
        label: "analyze",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">analyze</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AnalyzeFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an ANALYZE builder.<br>Example: analyze()->table(\'users\')<br>Produces: ANALYZE users
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\analyze()"),
        boost: 10
    },                {
        label: "any",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">any</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Any</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\any(" + "$" + "{" + "1:values" + "}" + ")"),
        boost: 10
    },                {
        label: "any_sub_select",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">any_sub_select</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ComparisonOperator</span> <span class=\"fn-param\">$operator</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SelectFinalStep</span> <span class=\"fn-param\">$subquery</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Any</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an ANY condition.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\any_sub_select(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:operator" + "}" + ", " + "$" + "{" + "3:subquery" + "}" + ")"),
        boost: 10
    },                {
        label: "append",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">append</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for save_mode_append().
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\append()"),
        boost: 10
    },                {
        label: "array_carrier",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_carrier</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$data</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayCarrier</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an ArrayCarrier.<br>Carrier backed by an associative array with case-insensitive key lookup.<br>@param array<string, string> $data Initial carrier data
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\array_carrier(" + "$" + "{" + "1:data" + "}" + ")"),
        boost: 10
    },                {
        label: "array_contained_by",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_contained_by</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OperatorCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an array is contained by condition (<@).<br>Example: array_contained_by(col(\'tags\'), raw_expr(\"ARRAY[\'sale\', \'featured\', \'new\']\"))<br>Produces: tags <@ ARRAY[\'sale\', \'featured\', \'new\']
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\array_contained_by(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "array_contains",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_contains</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OperatorCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an array contains condition (@>).<br>Example: array_contains(col(\'tags\'), raw_expr(\"ARRAY[\'sale\']\"))<br>Produces: tags @> ARRAY[\'sale\']
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\array_contains(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "array_exists",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_exists</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayPathExists</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $ref
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_exists(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:path" + "}" + ")"),
        boost: 10
    },                {
        label: "array_expand",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_expand</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ArrayExpand</span> <span class=\"fn-param\">$expand</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Function\\ArrayExpand\\ArrayExpand::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayExpand</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Expands each value into entry, if there are more than one value, multiple rows will be created.<br>Array keys are ignored, only values are used to create new rows.<br>Before:<br>  +--+-------------------+<br>  |id|              array|<br>  +--+-------------------+<br>  | 1|{\"a\":1,\"b\":2,\"c\":3}|<br>  +--+-------------------+<br>After:<br>  +--+--------+<br>  |id|expanded|<br>  +--+--------+<br>  | 1|       1|<br>  | 1|       2|<br>  | 1|       3|<br>  +--+--------+
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_expand(" + "$" + "{" + "1:function" + "}" + ", " + "$" + "{" + "2:expand" + "}" + ")"),
        boost: 10
    },                {
        label: "array_expr",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_expr</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$elements</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayExpression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an array expression.<br>@param list<Expression> $elements Array elements
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\array_expr(" + "$" + "{" + "1:elements" + "}" + ")"),
        boost: 10
    },                {
        label: "array_get",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_get</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayGet</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_get(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:path" + "}" + ")"),
        boost: 10
    },                {
        label: "array_get_collection",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_get_collection</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayGetCollection</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $keys
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_get_collection(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:keys" + "}" + ")"),
        boost: 10
    },                {
        label: "array_get_collection_first",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_get_collection_first</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayGetCollection</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_get_collection_first(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:keys" + "}" + ")"),
        boost: 10
    },                {
        label: "array_keys_style_convert",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_keys_style_convert</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StringStyles|StringStyles|string</span> <span class=\"fn-param\">$style</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\String\\StringStyles::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayKeysStyleConvert</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_keys_style_convert(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:style" + "}" + ")"),
        boost: 10
    },                {
        label: "array_key_rename",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_key_rename</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$newName</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayKeyRename</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_key_rename(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:path" + "}" + ", " + "$" + "{" + "3:newName" + "}" + ")"),
        boost: 10
    },                {
        label: "array_merge",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_merge</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayMerge</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $left<br>@param array<array-key, mixed>|ScalarFunction $right
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_merge(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "array_merge_collection",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_merge_collection</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$array</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayMergeCollection</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $array
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_merge_collection(" + "$" + "{" + "1:array" + "}" + ")"),
        boost: 10
    },                {
        label: "array_overlap",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_overlap</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OperatorCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an array overlap condition (&&).<br>Example: array_overlap(col(\'tags\'), raw_expr(\"ARRAY[\'sale\', \'featured\']\"))<br>Produces: tags && ARRAY[\'sale\', \'featured\']
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\array_overlap(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "array_reverse",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_reverse</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$preserveKeys</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayReverse</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $function
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_reverse(" + "$" + "{" + "1:function" + "}" + ", " + "$" + "{" + "2:preserveKeys" + "}" + ")"),
        boost: 10
    },                {
        label: "array_sort",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_sort</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|Sort|null</span> <span class=\"fn-param\">$sort_function</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int|null</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$recursive</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArraySort</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_sort(" + "$" + "{" + "1:function" + "}" + ", " + "$" + "{" + "2:sort_function" + "}" + ", " + "$" + "{" + "3:flags" + "}" + ", " + "$" + "{" + "4:recursive" + "}" + ")"),
        boost: 10
    },                {
        label: "array_to_generator",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_to_generator</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Generator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param array<T> $data<br>@return \\Generator<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Parquet\\array_to_generator(" + "$" + "{" + "1:data" + "}" + ")"),
        boost: 10
    },                {
        label: "array_to_row",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_to_row</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">EntryFactory</span> <span class=\"fn-param\">$entryFactory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Partitions|array</span> <span class=\"fn-param\">$partitions</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Row</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array<mixed>>|array<mixed|string> $data<br>@param array<Partition>|Partitions $partitions<br>@param null|Schema $schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_to_row(" + "$" + "{" + "1:data" + "}" + ", " + "$" + "{" + "2:entryFactory" + "}" + ", " + "$" + "{" + "3:partitions" + "}" + ", " + "$" + "{" + "4:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "array_to_rows",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_to_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">EntryFactory</span> <span class=\"fn-param\">$entryFactory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Partitions|array</span> <span class=\"fn-param\">$partitions</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Rows</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array<mixed>>|array<mixed|string> $data<br>@param array<Partition>|Partitions $partitions<br>@param null|Schema $schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_to_rows(" + "$" + "{" + "1:data" + "}" + ", " + "$" + "{" + "2:entryFactory" + "}" + ", " + "$" + "{" + "3:partitions" + "}" + ", " + "$" + "{" + "4:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "array_unpack",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">array_unpack</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$array</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$skip_keys</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string|null</span> <span class=\"fn-param\">$entry_prefix</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayUnpack</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $array<br>@param array<array-key, mixed>|ScalarFunction $skip_keys
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\array_unpack(" + "$" + "{" + "1:array" + "}" + ", " + "$" + "{" + "2:skip_keys" + "}" + ", " + "$" + "{" + "3:entry_prefix" + "}" + ")"),
        boost: 10
    },                {
        label: "asc",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">asc</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">NullsPosition</span> <span class=\"fn-param\">$nulls</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\PostgreSql\\QueryBuilder\\Clause\\NullsPosition::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OrderBy</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an ORDER BY item with ASC direction.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\asc(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:nulls" + "}" + ")"),
        boost: 10
    },                {
        label: "average",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">average</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$scale</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">2</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Rounding</span> <span class=\"fn-param\">$rounding</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Calculator\\Rounding::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Average</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\average(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:scale" + "}" + ", " + "$" + "{" + "3:rounding" + "}" + ")"),
        boost: 10
    },                {
        label: "aws_s3_client",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">aws_s3_client</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$configuration</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">S3Client</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string, mixed> $configuration - for details please see https://async-aws.com/clients/s3.html
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\Bridge\\AsyncAWS\\DSL\\aws_s3_client(" + "$" + "{" + "1:configuration" + "}" + ")"),
        boost: 10
    },                {
        label: "aws_s3_filesystem",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">aws_s3_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$bucket</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">S3Client</span> <span class=\"fn-param\">$s3Client</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Filesystem\\Bridge\\AsyncAWS\\Options::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AsyncAWSS3Filesystem</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\Bridge\\AsyncAWS\\DSL\\aws_s3_filesystem(" + "$" + "{" + "1:bucket" + "}" + ", " + "$" + "{" + "2:s3Client" + "}" + ", " + "$" + "{" + "3:options" + "}" + ")"),
        boost: 10
    },                {
        label: "azure_blob_service",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_blob_service</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Configuration</span> <span class=\"fn-param\">$configuration</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">AuthorizationFactory</span> <span class=\"fn-param\">$azure_authorization_factory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ClientInterface</span> <span class=\"fn-param\">$client</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">HttpFactory</span> <span class=\"fn-param\">$azure_http_factory</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">URLFactory</span> <span class=\"fn-param\">$azure_url_factory</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">LoggerInterface</span> <span class=\"fn-param\">$logger</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BlobServiceInterface</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Azure\\SDK\\DSL\\azure_blob_service(" + "$" + "{" + "1:configuration" + "}" + ", " + "$" + "{" + "2:azure_authorization_factory" + "}" + ", " + "$" + "{" + "3:client" + "}" + ", " + "$" + "{" + "4:azure_http_factory" + "}" + ", " + "$" + "{" + "5:azure_url_factory" + "}" + ", " + "$" + "{" + "6:logger" + "}" + ")"),
        boost: 10
    },                {
        label: "azure_blob_service_config",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_blob_service_config</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$account</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$container</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Configuration</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Azure\\SDK\\DSL\\azure_blob_service_config(" + "$" + "{" + "1:account" + "}" + ", " + "$" + "{" + "2:container" + "}" + ")"),
        boost: 10
    },                {
        label: "azure_filesystem",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">BlobServiceInterface</span> <span class=\"fn-param\">$blob_service</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Filesystem\\Bridge\\Azure\\Options::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AzureBlobFilesystem</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\Bridge\\Azure\\DSL\\azure_filesystem(" + "$" + "{" + "1:blob_service" + "}" + ", " + "$" + "{" + "2:options" + "}" + ")"),
        boost: 10
    },                {
        label: "azure_filesystem_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_filesystem_options</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Options</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\Bridge\\Azure\\DSL\\azure_filesystem_options()"),
        boost: 10
    },                {
        label: "azure_http_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_http_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">RequestFactoryInterface</span> <span class=\"fn-param\">$request_factory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StreamFactoryInterface</span> <span class=\"fn-param\">$stream_factory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HttpFactory</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Azure\\SDK\\DSL\\azure_http_factory(" + "$" + "{" + "1:request_factory" + "}" + ", " + "$" + "{" + "2:stream_factory" + "}" + ")"),
        boost: 10
    },                {
        label: "azure_shared_key_authorization_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_shared_key_authorization_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$account</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$key</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SharedKeyFactory</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Azure\\SDK\\DSL\\azure_shared_key_authorization_factory(" + "$" + "{" + "1:account" + "}" + ", " + "$" + "{" + "2:key" + "}" + ")"),
        boost: 10
    },                {
        label: "azure_url_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azure_url_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$host</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;blob.core.windows.net&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AzureURLFactory</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Azure\\SDK\\DSL\\azure_url_factory(" + "$" + "{" + "1:host" + "}" + ")"),
        boost: 10
    },                {
        label: "azurite_url_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">azurite_url_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$host</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;localhost&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$port</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;10000&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$secure</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AzuriteURLFactory</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Azure\\SDK\\DSL\\azurite_url_factory(" + "$" + "{" + "1:host" + "}" + ", " + "$" + "{" + "2:port" + "}" + ", " + "$" + "{" + "3:secure" + "}" + ")"),
        boost: 10
    },                {
        label: "baggage",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">baggage</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$entries</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Baggage</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a Baggage.<br>@param array<string, string> $entries Initial key-value entries
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\baggage(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },                {
        label: "bar_chart",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">bar_chart</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference</span> <span class=\"fn-param\">$label</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">References</span> <span class=\"fn-param\">$datasets</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BarChart</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\ChartJS\\bar_chart(" + "$" + "{" + "1:label" + "}" + ", " + "$" + "{" + "2:datasets" + "}" + ")"),
        boost: 10
    },                {
        label: "batched_by",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">batched_by</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$min_size</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchByExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param null|int<1, max> $min_size
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\batched_by(" + "$" + "{" + "1:extractor" + "}" + ", " + "$" + "{" + "2:column" + "}" + ", " + "$" + "{" + "3:min_size" + "}" + ")"),
        boost: 10
    },                {
        label: "batches",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">batches</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$size</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param int<1, max> $size
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\batches(" + "$" + "{" + "1:extractor" + "}" + ", " + "$" + "{" + "2:size" + "}" + ")"),
        boost: 10
    },                {
        label: "batching_log_processor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">batching_log_processor</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">LogExporter</span> <span class=\"fn-param\">$exporter</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$batchSize</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">512</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchingLogProcessor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a BatchingLogProcessor.<br>Collects log records in memory and exports them in batches for efficiency.<br>Logs are exported when batch size is reached, flush() is called, or shutdown().<br>@param LogExporter $exporter The exporter to send logs to<br>@param int $batchSize Number of logs to collect before exporting (default 512)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\batching_log_processor(" + "$" + "{" + "1:exporter" + "}" + ", " + "$" + "{" + "2:batchSize" + "}" + ")"),
        boost: 10
    },                {
        label: "batching_metric_processor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">batching_metric_processor</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">MetricExporter</span> <span class=\"fn-param\">$exporter</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$batchSize</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">512</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchingMetricProcessor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a BatchingMetricProcessor.<br>Collects metrics in memory and exports them in batches for efficiency.<br>Metrics are exported when batch size is reached, flush() is called, or shutdown().<br>@param MetricExporter $exporter The exporter to send metrics to<br>@param int $batchSize Number of metrics to collect before exporting (default 512)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\batching_metric_processor(" + "$" + "{" + "1:exporter" + "}" + ", " + "$" + "{" + "2:batchSize" + "}" + ")"),
        boost: 10
    },                {
        label: "batching_span_processor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">batching_span_processor</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SpanExporter</span> <span class=\"fn-param\">$exporter</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$batchSize</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">512</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchingSpanProcessor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a BatchingSpanProcessor.<br>Collects spans in memory and exports them in batches for efficiency.<br>Spans are exported when batch size is reached, flush() is called, or shutdown().<br>@param SpanExporter $exporter The exporter to send spans to<br>@param int $batchSize Number of spans to collect before exporting (default 512)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\batching_span_processor(" + "$" + "{" + "1:exporter" + "}" + ", " + "$" + "{" + "2:batchSize" + "}" + ")"),
        boost: 10
    },                {
        label: "batch_size",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">batch_size</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$size</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchSize</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param int<1, max> $size
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\batch_size(" + "$" + "{" + "1:size" + "}" + ")"),
        boost: 10
    },                {
        label: "begin",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">begin</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BeginOptionsStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a BEGIN transaction builder.<br>Example: begin()->isolationLevel(IsolationLevel::SERIALIZABLE)->readOnly()<br>Produces: BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\begin()"),
        boost: 10
    },                {
        label: "between",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">between</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$lower_bound</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$upper_bound</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|Boundary</span> <span class=\"fn-param\">$boundary</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Function\\Between\\Boundary::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Between</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\between(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:lower_bound" + "}" + ", " + "$" + "{" + "3:upper_bound" + "}" + ", " + "$" + "{" + "4:boundary" + "}" + ")"),
        boost: 10
    },                {
        label: "between",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">between</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$low</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$high</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$not</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Between</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a BETWEEN condition.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\between(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:low" + "}" + ", " + "$" + "{" + "3:high" + "}" + ", " + "$" + "{" + "4:not" + "}" + ")"),
        boost: 10
    },                {
        label: "binary_expr",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">binary_expr</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$operator</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BinaryExpression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a binary expression (left op right).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\binary_expr(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:operator" + "}" + ", " + "$" + "{" + "3:right" + "}" + ")"),
        boost: 10
    },                {
        label: "boolean_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">boolean_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?bool>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\boolean_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "bool_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">bool_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?bool>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\bool_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "bool_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">bool_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BooleanDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\bool_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "bulk_insert",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">bulk_insert</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$columns</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$rowCount</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BulkInsert</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an optimized bulk INSERT query for high-performance multi-row inserts.<br>Unlike insert() which uses immutable builder patterns (O(n²) for n rows),<br>this function generates SQL directly using string operations (O(n) complexity).<br>@param string $table Table name<br>@param list<string> $columns Column names<br>@param int $rowCount Number of rows to insert
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\bulk_insert(" + "$" + "{" + "1:table" + "}" + ", " + "$" + "{" + "2:columns" + "}" + ", " + "$" + "{" + "3:rowCount" + "}" + ")"),
        boost: 10
    },                {
        label: "caching_detector",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">caching_detector</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ResourceDetector</span> <span class=\"fn-param\">$detector</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$cachePath</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CachingDetector</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a CachingDetector.<br>Wraps another detector and caches its results to a file. On subsequent<br>calls, returns the cached resource instead of running detection again.<br>@param ResourceDetector $detector The detector to wrap<br>@param null|string $cachePath Cache file path (default: sys_get_temp_dir()/flow_telemetry_resource.cache)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\caching_detector(" + "$" + "{" + "1:detector" + "}" + ", " + "$" + "{" + "2:cachePath" + "}" + ")"),
        boost: 10
    },                {
        label: "call",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">call</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|callable</span> <span class=\"fn-param\">$callable</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$return_type</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CallUserFunc</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Calls a user-defined function with the given parameters.<br>@param callable|ScalarFunction $callable<br>@param array<mixed> $parameters<br>@param null|Type<mixed> $return_type
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\call(" + "$" + "{" + "1:callable" + "}" + ", " + "$" + "{" + "2:parameters" + "}" + ", " + "$" + "{" + "3:return_type" + "}" + ")"),
        boost: 10
    },                {
        label: "call",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">call</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$procedure</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CallFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Creates a CALL statement builder for invoking a procedure.<br>Example: call(\'update_stats\')->with(123)<br>Produces: CALL update_stats(123)<br>Example: call(\'process_data\')->with(\'test\', 42, true)<br>Produces: CALL process_data(\'test\', 42, true)<br>@param string $procedure The name of the procedure to call<br>@return CallFinalStep Builder for call statement options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\call(" + "$" + "{" + "1:procedure" + "}" + ")"),
        boost: 10
    },                {
        label: "capitalize",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">capitalize</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Capitalize</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\capitalize(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },                {
        label: "case_when",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">case_when</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$whenClauses</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$elseResult</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$operand</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CaseExpression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a CASE expression.<br>@param non-empty-list<WhenClause> $whenClauses WHEN clauses<br>@param null|Expression $elseResult ELSE result (optional)<br>@param null|Expression $operand CASE operand for simple CASE (optional)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\case_when(" + "$" + "{" + "1:whenClauses" + "}" + ", " + "$" + "{" + "2:elseResult" + "}" + ", " + "$" + "{" + "3:operand" + "}" + ")"),
        boost: 10
    },                {
        label: "cast",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">cast</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type|string</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Cast</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param \\Flow\\Types\\Type<mixed>|string $type
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\cast(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:type" + "}" + ")"),
        boost: 10
    },                {
        label: "cast",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">cast</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DataType</span> <span class=\"fn-param\">$dataType</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TypeCast</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a type cast expression.<br>@param Expression $expr Expression to cast<br>@param DataType $dataType Target data type (use data_type_* functions)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\cast(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:dataType" + "}" + ")"),
        boost: 10
    },                {
        label: "chain_detector",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">chain_detector</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ResourceDetector</span> <span class=\"fn-param\">$detectors</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChainDetector</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a ChainDetector.<br>Combines multiple resource detectors into a chain. Detectors are executed<br>in order and their results are merged. Later detectors take precedence<br>over earlier ones when there are conflicting attribute keys.<br>@param ResourceDetector ...$detectors The detectors to chain
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\chain_detector(" + "$" + "{" + "1:detectors" + "}" + ")"),
        boost: 10
    },                {
        label: "check_constraint",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">check_constraint</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$expression</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CheckConstraint</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a CHECK constraint.<br>@param string $expression SQL expression that must evaluate to true
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\check_constraint(" + "$" + "{" + "1:expression" + "}" + ")"),
        boost: 10
    },                {
        label: "chunks_from",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">chunks_from</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$chunk_size</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BatchExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param int<1, max> $chunk_size<br>@deprecated use batches() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\chunks_from(" + "$" + "{" + "1:extractor" + "}" + ", " + "$" + "{" + "2:chunk_size" + "}" + ")"),
        boost: 10
    },                {
        label: "close_cursor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">close_cursor</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$cursorName</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CloseCursorFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Close a cursor.<br>Example: close_cursor(\'my_cursor\')<br>Produces: CLOSE my_cursor<br>Example: close_cursor() - closes all cursors<br>Produces: CLOSE ALL<br>@param null|string $cursorName Cursor to close, or null to close all
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\close_cursor(" + "$" + "{" + "1:cursorName" + "}" + ")"),
        boost: 10
    },                {
        label: "cluster",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">cluster</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ClusterFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a CLUSTER builder.<br>Example: cluster()->table(\'users\')->using(\'idx_users_pkey\')<br>Produces: CLUSTER users USING idx_users_pkey
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\cluster()"),
        boost: 10
    },                {
        label: "coalesce",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">coalesce</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Coalesce</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\coalesce(" + "$" + "{" + "1:values" + "}" + ")"),
        boost: 10
    },                {
        label: "coalesce",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">coalesce</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expressions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Coalesce</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a COALESCE expression.<br>@param Expression ...$expressions Expressions to coalesce
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\coalesce(" + "$" + "{" + "1:expressions" + "}" + ")"),
        boost: 10
    },                {
        label: "col",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">col</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EntryReference</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    An alias for \`ref\`.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\col(" + "$" + "{" + "1:entry" + "}" + ")"),
        boost: 10
    },                {
        label: "col",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">col</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$table</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Column</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a column reference expression.<br>Can be used in two modes:<br>- Parse mode: col(\'users.id\') or col(\'schema.table.column\') - parses dot-separated string<br>- Explicit mode: col(\'id\', \'users\') or col(\'id\', \'users\', \'schema\') - separate arguments<br>When $table or $schema is provided, $column must be a plain column name (no dots).<br>@param string $column Column name, or dot-separated path like \"table.column\" or \"schema.table.column\"<br>@param null|string $table Table name (optional, triggers explicit mode)<br>@param null|string $schema Schema name (optional, requires $table)<br>@throws InvalidExpressionException when $schema is provided without $table, or when $column contains dots in explicit mode
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\col(" + "$" + "{" + "1:column" + "}" + ", " + "$" + "{" + "2:table" + "}" + ", " + "$" + "{" + "3:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "collect",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">collect</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Collect</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\collect(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },                {
        label: "collect_unique",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">collect_unique</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CollectUnique</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\collect_unique(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },                {
        label: "column",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">column</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DataType</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ColumnDefinition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a column definition for CREATE TABLE.<br>@param string $name Column name<br>@param DataType $type Column data type
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\column(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:type" + "}" + ")"),
        boost: 10
    },                {
        label: "combine",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">combine</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Combine</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed>|ScalarFunction $keys<br>@param array<array-key, mixed>|ScalarFunction $values
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\combine(" + "$" + "{" + "1:keys" + "}" + ", " + "$" + "{" + "2:values" + "}" + ")"),
        boost: 10
    },                {
        label: "comment",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">comment</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">CommentTarget</span> <span class=\"fn-param\">$target</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CommentFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a COMMENT ON builder.<br>Example: comment(CommentTarget::TABLE, \'users\')->is(\'User accounts table\')<br>Produces: COMMENT ON TABLE users IS \'User accounts table\'<br>@param CommentTarget $target Target type (TABLE, COLUMN, INDEX, etc.)<br>@param string $name Target name (use \'table.column\' for COLUMN targets)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\comment(" + "$" + "{" + "1:target" + "}" + ", " + "$" + "{" + "2:name" + "}" + ")"),
        boost: 10
    },                {
        label: "commit",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">commit</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CommitOptionsStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a COMMIT transaction builder.<br>Example: commit()->andChain()<br>Produces: COMMIT AND CHAIN
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\commit()"),
        boost: 10
    },                {
        label: "commit_prepared",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">commit_prepared</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$transactionId</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PreparedTransactionFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a COMMIT PREPARED builder.<br>Example: commit_prepared(\'my_transaction\')<br>Produces: COMMIT PREPARED \'my_transaction\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\commit_prepared(" + "$" + "{" + "1:transactionId" + "}" + ")"),
        boost: 10
    },                {
        label: "compare_all",
        type: "function",
        detail: "flow\u002Ddsl\u002Dcomparisons",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Comparison</span> <span class=\"fn-param\">$comparisons</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">All</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_all(" + "$" + "{" + "1:comparisons" + "}" + ")"),
        boost: 10
    },                {
        label: "compare_any",
        type: "function",
        detail: "flow\u002Ddsl\u002Dcomparisons",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_any</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Comparison</span> <span class=\"fn-param\">$comparisons</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Any</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_any(" + "$" + "{" + "1:comparisons" + "}" + ")"),
        boost: 10
    },                {
        label: "compare_entries_by_name",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_entries_by_name</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Order</span> <span class=\"fn-param\">$order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Transformer\\OrderEntries\\Order::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_entries_by_name(" + "$" + "{" + "1:order" + "}" + ")"),
        boost: 10
    },                {
        label: "compare_entries_by_name_desc",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_entries_by_name_desc</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_entries_by_name_desc()"),
        boost: 10
    },                {
        label: "compare_entries_by_type",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_entries_by_type</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$priorities</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[...]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Order</span> <span class=\"fn-param\">$order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Transformer\\OrderEntries\\Order::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<class-string<Entry<mixed>>, int> $priorities
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_entries_by_type(" + "$" + "{" + "1:priorities" + "}" + ", " + "$" + "{" + "2:order" + "}" + ")"),
        boost: 10
    },                {
        label: "compare_entries_by_type_and_name",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_entries_by_type_and_name</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$priorities</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[...]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Order</span> <span class=\"fn-param\">$order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Transformer\\OrderEntries\\Order::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<class-string<Entry<mixed>>, int> $priorities
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_entries_by_type_and_name(" + "$" + "{" + "1:priorities" + "}" + ", " + "$" + "{" + "2:order" + "}" + ")"),
        boost: 10
    },                {
        label: "compare_entries_by_type_desc",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">compare_entries_by_type_desc</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$priorities</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[...]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<class-string<Entry<mixed>>, int> $priorities
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\compare_entries_by_type_desc(" + "$" + "{" + "1:priorities" + "}" + ")"),
        boost: 10
    },                {
        label: "composer_detector",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">composer_detector</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ComposerDetector</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a ComposerDetector.<br>Detects service.name and service.version from Composer\'s InstalledVersions<br>using the root package information.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\composer_detector()"),
        boost: 10
    },                {
        label: "composite_propagator",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">composite_propagator</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Propagator</span> <span class=\"fn-param\">$propagators</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CompositePropagator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a CompositePropagator.<br>Combines multiple propagators into one. On extract, all propagators are<br>invoked and their contexts are merged. On inject, all propagators are invoked.<br>@param Propagator ...$propagators The propagators to combine
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\composite_propagator(" + "$" + "{" + "1:propagators" + "}" + ")"),
        boost: 10
    },                {
        label: "concat",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">concat</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$functions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Concat</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Concat all values. If you want to concatenate values with separator use concat_ws function.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\concat(" + "$" + "{" + "1:functions" + "}" + ")"),
        boost: 10
    },                {
        label: "concat_ws",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">concat_ws</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$separator</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$functions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConcatWithSeparator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Concat all values with separator.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\concat_ws(" + "$" + "{" + "1:separator" + "}" + ", " + "$" + "{" + "2:functions" + "}" + ")"),
        boost: 10
    },                {
        label: "conditions",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">conditions</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConditionBuilder</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a condition builder for fluent condition composition.<br>This builder allows incremental condition building with a fluent API:<br>\`\`\`php<br>$builder = conditions();<br>if ($hasFilter) {<br>    $builder = $builder->and(eq(col(\'status\'), literal(\'active\')));<br>}<br>if (!$builder->isEmpty()) {<br>    $query = select()->from(table(\'users\'))->where($builder);<br>}<br>\`\`\`
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\conditions()"),
        boost: 10
    },                {
        label: "cond_and",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">cond_and</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Condition</span> <span class=\"fn-param\">$conditions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AndCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Combine conditions with AND.<br>@param Condition ...$conditions Conditions to combine
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\cond_and(" + "$" + "{" + "1:conditions" + "}" + ")"),
        boost: 10
    },                {
        label: "cond_false",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">cond_false</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RawCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a FALSE condition for WHERE clauses.<br>Useful when you need a condition that always evaluates to false,<br>typically for testing or to return an empty result set.<br>Example: select(literal(1))->where(cond_false()) // SELECT 1 WHERE false
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\cond_false()"),
        boost: 10
    },                {
        label: "cond_not",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">cond_not</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Condition</span> <span class=\"fn-param\">$condition</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">NotCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Negate a condition with NOT.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\cond_not(" + "$" + "{" + "1:condition" + "}" + ")"),
        boost: 10
    },                {
        label: "cond_or",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">cond_or</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Condition</span> <span class=\"fn-param\">$conditions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OrCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Combine conditions with OR.<br>@param Condition ...$conditions Conditions to combine
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\cond_or(" + "$" + "{" + "1:conditions" + "}" + ")"),
        boost: 10
    },                {
        label: "cond_true",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">cond_true</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RawCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a TRUE condition for WHERE clauses.<br>Useful when you need a condition that always evaluates to true.<br>Example: select(literal(1))->where(cond_true()) // SELECT 1 WHERE true
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\cond_true()"),
        boost: 10
    },                {
        label: "config",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">config</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Config</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\config()"),
        boost: 10
    },                {
        label: "config_builder",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">config_builder</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConfigBuilder</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\config_builder()"),
        boost: 10
    },                {
        label: "conflict_columns",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">conflict_columns</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$columns</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConflictTarget</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a conflict target for ON CONFLICT (columns).<br>@param list<string> $columns Columns that define uniqueness
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\conflict_columns(" + "$" + "{" + "1:columns" + "}" + ")"),
        boost: 10
    },                {
        label: "conflict_constraint",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">conflict_constraint</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConflictTarget</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a conflict target for ON CONFLICT ON CONSTRAINT.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\conflict_constraint(" + "$" + "{" + "1:name" + "}" + ")"),
        boost: 10
    },                {
        label: "console_log_exporter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">console_log_exporter</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$colors</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$maxBodyLength</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">100</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConsoleLogExporter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a ConsoleLogExporter.<br>Outputs log records to the console with severity-based coloring.<br>Useful for debugging and development.<br>@param bool $colors Whether to use ANSI colors (default: true)<br>@param null|int $maxBodyLength Maximum length for body+attributes column (null = no limit, default: 100)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\console_log_exporter(" + "$" + "{" + "1:colors" + "}" + ", " + "$" + "{" + "2:maxBodyLength" + "}" + ")"),
        boost: 10
    },                {
        label: "console_metric_exporter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">console_metric_exporter</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$colors</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConsoleMetricExporter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a ConsoleMetricExporter.<br>Outputs metrics to the console with ASCII table formatting.<br>Useful for debugging and development.<br>@param bool $colors Whether to use ANSI colors (default: true)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\console_metric_exporter(" + "$" + "{" + "1:colors" + "}" + ")"),
        boost: 10
    },                {
        label: "console_span_exporter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">console_span_exporter</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$colors</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConsoleSpanExporter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a ConsoleSpanExporter.<br>Outputs spans to the console with ASCII table formatting.<br>Useful for debugging and development.<br>@param bool $colors Whether to use ANSI colors (default: true)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\console_span_exporter(" + "$" + "{" + "1:colors" + "}" + ")"),
        boost: 10
    },                {
        label: "constraint_sorted_by",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">constraint_sorted_by</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$columns</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SortedByConstraint</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\constraint_sorted_by(" + "$" + "{" + "1:column" + "}" + ", " + "$" + "{" + "2:columns" + "}" + ")"),
        boost: 10
    },                {
        label: "constraint_unique",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">constraint_unique</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$reference</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$references</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">UniqueConstraint</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\constraint_unique(" + "$" + "{" + "1:reference" + "}" + ", " + "$" + "{" + "2:references" + "}" + ")"),
        boost: 10
    },                {
        label: "context",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">context</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">TraceId</span> <span class=\"fn-param\">$traceId</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Baggage</span> <span class=\"fn-param\">$baggage</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Context</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a Context.<br>If no TraceId is provided, generates a new one.<br>If no Baggage is provided, creates an empty one.<br>@param null|TraceId $traceId Optional TraceId to use<br>@param null|Baggage $baggage Optional Baggage to use
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\context(" + "$" + "{" + "1:traceId" + "}" + ", " + "$" + "{" + "2:baggage" + "}" + ")"),
        boost: 10
    },                {
        label: "copy",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">copy</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CopyFactory</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a new COPY query builder for data import/export.<br>Usage:<br>  copy()->from(\'users\')->file(\'/tmp/users.csv\')->format(CopyFormat::CSV)<br>  copy()->to(\'users\')->file(\'/tmp/users.csv\')->format(CopyFormat::CSV)<br>  copy()->toQuery(select(...))->file(\'/tmp/data.csv\')
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\copy()"),
        boost: 10
    },                {
        label: "count",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">count</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference</span> <span class=\"fn-param\">$function</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Count</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\count(" + "$" + "{" + "1:function" + "}" + ")"),
        boost: 10
    },                {
        label: "create",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">create</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CreateFactory</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a factory for building CREATE statements.<br>Provides a unified entry point for all CREATE operations:<br>- create()->table() - CREATE TABLE<br>- create()->tableAs() - CREATE TABLE AS<br>- create()->index() - CREATE INDEX<br>- create()->view() - CREATE VIEW<br>- create()->materializedView() - CREATE MATERIALIZED VIEW<br>- create()->sequence() - CREATE SEQUENCE<br>- create()->schema() - CREATE SCHEMA<br>- create()->role() - CREATE ROLE<br>- create()->function() - CREATE FUNCTION<br>- create()->procedure() - CREATE PROCEDURE<br>- create()->trigger() - CREATE TRIGGER<br>- create()->rule() - CREATE RULE<br>- create()->extension() - CREATE EXTENSION<br>- create()->compositeType() - CREATE TYPE (composite)<br>- create()->enumType() - CREATE TYPE (enum)<br>- create()->rangeType() - CREATE TYPE (range)<br>- create()->domain() - CREATE DOMAIN<br>Example: create()->table(\'users\')->columns(col_def(\'id\', data_type_serial()))<br>Example: create()->index(\'idx_email\')->on(\'users\')->columns(\'email\')
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\create()"),
        boost: 10
    },                {
        label: "csv_detect_separator",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">csv_detect_separator</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SourceStream</span> <span class=\"fn-param\">$stream</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$lines</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">5</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Option</span> <span class=\"fn-param\">$fallback</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Adapter\\CSV\\Detector\\Option::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Option</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param SourceStream $stream - valid resource to CSV file<br>@param int<1, max> $lines - number of lines to read from CSV file, default 5, more lines means more accurate detection but slower detection<br>@param null|Option $fallback - fallback option to use when no best option can be detected, default is Option(\',\', \'\"\', \'\\\\\')<br>@param null|Options $options - options to use for detection, default is Options::all()
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\CSV\\csv_detect_separator(" + "$" + "{" + "1:stream" + "}" + ", " + "$" + "{" + "2:lines" + "}" + ", " + "$" + "{" + "3:fallback" + "}" + ", " + "$" + "{" + "4:options" + "}" + ")"),
        boost: 10
    },                {
        label: "cte",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">cte</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SelectFinalStep</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$columnNames</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">CTEMaterialization</span> <span class=\"fn-param\">$materialization</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\PostgreSql\\QueryBuilder\\Clause\\CTEMaterialization::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$recursive</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CTE</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a CTE (Common Table Expression).<br>@param string $name CTE name<br>@param SelectFinalStep $query CTE query<br>@param array<string> $columnNames Column aliases (optional)<br>@param CTEMaterialization $materialization Materialization hint
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\cte(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:query" + "}" + ", " + "$" + "{" + "3:columnNames" + "}" + ", " + "$" + "{" + "4:materialization" + "}" + ", " + "$" + "{" + "5:recursive" + "}" + ")"),
        boost: 10
    },                {
        label: "current_date",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">current_date</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SQLValueFunctionExpression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    SQL standard CURRENT_DATE function.<br>Returns the current date (at the start of the transaction).<br>Useful as a column default value or in SELECT queries.<br>Example: column(\'birth_date\', data_type_date())->default(current_date())<br>Example: select()->select(current_date()->as(\'today\'))
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\current_date()"),
        boost: 10
    },                {
        label: "current_time",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">current_time</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SQLValueFunctionExpression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    SQL standard CURRENT_TIME function.<br>Returns the current time (at the start of the transaction).<br>Useful as a column default value or in SELECT queries.<br>Example: column(\'start_time\', data_type_time())->default(current_time())<br>Example: select()->select(current_time()->as(\'now_time\'))
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\current_time()"),
        boost: 10
    },                {
        label: "current_timestamp",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">current_timestamp</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SQLValueFunctionExpression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    SQL standard CURRENT_TIMESTAMP function.<br>Returns the current date and time (at the start of the transaction).<br>Useful as a column default value or in SELECT queries.<br>Example: column(\'created_at\', data_type_timestamp())->default(current_timestamp())<br>Example: select()->select(current_timestamp()->as(\'now\'))
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\current_timestamp()"),
        boost: 10
    },                {
        label: "data_frame",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_frame</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Config|ConfigBuilder|null</span> <span class=\"fn-param\">$config</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Flow</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\data_frame(" + "$" + "{" + "1:config" + "}" + ")"),
        boost: 10
    },                {
        label: "data_type_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_array</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DataType</span> <span class=\"fn-param\">$elementType</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an array data type from an element type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_array(" + "$" + "{" + "1:elementType" + "}" + ")"),
        boost: 10
    },                {
        label: "data_type_bigint",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_bigint</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a bigint data type (PostgreSQL int8).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_bigint()"),
        boost: 10
    },                {
        label: "data_type_bigserial",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_bigserial</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a bigserial data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_bigserial()"),
        boost: 10
    },                {
        label: "data_type_boolean",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_boolean</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a boolean data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_boolean()"),
        boost: 10
    },                {
        label: "data_type_bytea",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_bytea</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a bytea data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_bytea()"),
        boost: 10
    },                {
        label: "data_type_char",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_char</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$length</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a char data type with length constraint.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_char(" + "$" + "{" + "1:length" + "}" + ")"),
        boost: 10
    },                {
        label: "data_type_cidr",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_cidr</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a cidr data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_cidr()"),
        boost: 10
    },                {
        label: "data_type_custom",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_custom</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$typeName</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a custom data type.<br>@param string $typeName Type name<br>@param null|string $schema Optional schema name
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_custom(" + "$" + "{" + "1:typeName" + "}" + ", " + "$" + "{" + "2:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "data_type_date",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_date</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a date data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_date()"),
        boost: 10
    },                {
        label: "data_type_decimal",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_decimal</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$precision</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$scale</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a decimal data type with optional precision and scale.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_decimal(" + "$" + "{" + "1:precision" + "}" + ", " + "$" + "{" + "2:scale" + "}" + ")"),
        boost: 10
    },                {
        label: "data_type_double_precision",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_double_precision</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a double precision data type (PostgreSQL float8).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_double_precision()"),
        boost: 10
    },                {
        label: "data_type_inet",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_inet</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an inet data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_inet()"),
        boost: 10
    },                {
        label: "data_type_integer",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_integer</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an integer data type (PostgreSQL int4).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_integer()"),
        boost: 10
    },                {
        label: "data_type_interval",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_interval</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an interval data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_interval()"),
        boost: 10
    },                {
        label: "data_type_json",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_json</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a JSON data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_json()"),
        boost: 10
    },                {
        label: "data_type_jsonb",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_jsonb</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a JSONB data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_jsonb()"),
        boost: 10
    },                {
        label: "data_type_macaddr",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_macaddr</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a macaddr data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_macaddr()"),
        boost: 10
    },                {
        label: "data_type_numeric",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_numeric</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$precision</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$scale</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a numeric data type with optional precision and scale.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_numeric(" + "$" + "{" + "1:precision" + "}" + ", " + "$" + "{" + "2:scale" + "}" + ")"),
        boost: 10
    },                {
        label: "data_type_real",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_real</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a real data type (PostgreSQL float4).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_real()"),
        boost: 10
    },                {
        label: "data_type_serial",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_serial</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a serial data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_serial()"),
        boost: 10
    },                {
        label: "data_type_smallint",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_smallint</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a smallint data type (PostgreSQL int2).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_smallint()"),
        boost: 10
    },                {
        label: "data_type_smallserial",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_smallserial</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a smallserial data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_smallserial()"),
        boost: 10
    },                {
        label: "data_type_text",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_text</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a text data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_text()"),
        boost: 10
    },                {
        label: "data_type_time",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_time</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$precision</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a time data type with optional precision.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_time(" + "$" + "{" + "1:precision" + "}" + ")"),
        boost: 10
    },                {
        label: "data_type_timestamp",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_timestamp</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$precision</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a timestamp data type with optional precision.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_timestamp(" + "$" + "{" + "1:precision" + "}" + ")"),
        boost: 10
    },                {
        label: "data_type_timestamptz",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_timestamptz</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$precision</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a timestamp with time zone data type with optional precision.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_timestamptz(" + "$" + "{" + "1:precision" + "}" + ")"),
        boost: 10
    },                {
        label: "data_type_uuid",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_uuid</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a UUID data type.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_uuid()"),
        boost: 10
    },                {
        label: "data_type_varchar",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">data_type_varchar</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$length</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a varchar data type with length constraint.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\data_type_varchar(" + "$" + "{" + "1:length" + "}" + ")"),
        boost: 10
    },                {
        label: "datetime_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">datetime_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\DateTimeInterface>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\datetime_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "datetime_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">datetime_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DateTimeDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\datetime_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "date_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">date_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\DateTimeInterface>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\date_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "date_interval_to_microseconds",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">date_interval_to_microseconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\date_interval_to_microseconds(" + "$" + "{" + "1:interval" + "}" + ")"),
        boost: 10
    },                {
        label: "date_interval_to_milliseconds",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">date_interval_to_milliseconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\date_interval_to_milliseconds(" + "$" + "{" + "1:interval" + "}" + ")"),
        boost: 10
    },                {
        label: "date_interval_to_seconds",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">date_interval_to_seconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\date_interval_to_seconds(" + "$" + "{" + "1:interval" + "}" + ")"),
        boost: 10
    },                {
        label: "date_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">date_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DateDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\date_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "date_time_format",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">date_time_format</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$format</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DateTimeFormat</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\date_time_format(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:format" + "}" + ")"),
        boost: 10
    },                {
        label: "dbal_dataframe_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dbal_dataframe_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">QueryParameter</span> <span class=\"fn-param\">$parameters</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalDataFrameFactory</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string, mixed>|Connection $connection<br>@param string $query<br>@param QueryParameter ...$parameters
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\dbal_dataframe_factory(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:query" + "}" + ", " + "$" + "{" + "3:parameters" + "}" + ")"),
        boost: 10
    },                {
        label: "dbal_from_queries",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dbal_from_queries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ParametersSet</span> <span class=\"fn-param\">$parameters_set</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalQueryExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @deprecated use from_dbal_queries() instead<br>@param null|ParametersSet $parameters_set - each one parameters array will be evaluated as new query<br>@param array<int|string, DbalArrayType|DbalParameterType|DbalType|int|string> $types
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\dbal_from_queries(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:query" + "}" + ", " + "$" + "{" + "3:parameters_set" + "}" + ", " + "$" + "{" + "4:types" + "}" + ")"),
        boost: 10
    },                {
        label: "dbal_from_query",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dbal_from_query</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalQueryExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @deprecated use from_dbal_query() instead<br>@param array<string, mixed>|list<mixed> $parameters - @deprecated use DbalQueryExtractor::withParameters() instead<br>@param array<int<0, max>|string, DbalArrayType|DbalParameterType|DbalType|string> $types - @deprecated use DbalQueryExtractor::withTypes() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\dbal_from_query(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:query" + "}" + ", " + "$" + "{" + "3:parameters" + "}" + ", " + "$" + "{" + "4:types" + "}" + ")"),
        boost: 10
    },                {
        label: "declare_cursor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">declare_cursor</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$cursorName</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SelectFinalStep|SqlQuery|string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DeclareCursorOptionsStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Declare a server-side cursor for a query.<br>Cursors must be declared within a transaction and provide memory-efficient<br>iteration over large result sets via FETCH commands.<br>Example with query builder:<br>  declare_cursor(\'my_cursor\', select(star())->from(table(\'users\')))->noScroll()<br>  Produces: DECLARE my_cursor NO SCROLL CURSOR FOR SELECT * FROM users<br>Example with raw SQL:<br>  declare_cursor(\'my_cursor\', \'SELECT * FROM users WHERE active = true\')->withHold()<br>  Produces: DECLARE my_cursor NO SCROLL CURSOR WITH HOLD FOR SELECT * FROM users WHERE active = true<br>@param string $cursorName Unique cursor name<br>@param SelectFinalStep|SqlQuery|string $query Query to iterate over
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\declare_cursor(" + "$" + "{" + "1:cursorName" + "}" + ", " + "$" + "{" + "2:query" + "}" + ")"),
        boost: 10
    },                {
        label: "definition_from_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">definition_from_array</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$definition</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a Definition from an array representation.<br>@param array<array-key, mixed> $definition<br>@return Definition<mixed>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\definition_from_array(" + "$" + "{" + "1:definition" + "}" + ")"),
        boost: 10
    },                {
        label: "definition_from_type",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">definition_from_type</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Definition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a Definition from a Type.<br>@param Type<mixed> $type<br>@return Definition<mixed>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\definition_from_type(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:type" + "}" + ", " + "$" + "{" + "3:nullable" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "delay_exponential",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">delay_exponential</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$base</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$multiplier</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">2</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$max_delay</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Exponential</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\delay_exponential(" + "$" + "{" + "1:base" + "}" + ", " + "$" + "{" + "2:multiplier" + "}" + ", " + "$" + "{" + "3:max_delay" + "}" + ")"),
        boost: 10
    },                {
        label: "delay_fixed",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">delay_fixed</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$delay</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Fixed</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\delay_fixed(" + "$" + "{" + "1:delay" + "}" + ")"),
        boost: 10
    },                {
        label: "delay_jitter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">delay_jitter</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DelayFactory</span> <span class=\"fn-param\">$delay</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">float</span> <span class=\"fn-param\">$jitter_factor</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Jitter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param float $jitter_factor a value between 0 and 1 representing the maximum percentage of jitter to apply
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\delay_jitter(" + "$" + "{" + "1:delay" + "}" + ", " + "$" + "{" + "2:jitter_factor" + "}" + ")"),
        boost: 10
    },                {
        label: "delay_linear",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">delay_linear</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$delay</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Duration</span> <span class=\"fn-param\">$increment</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Linear</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\delay_linear(" + "$" + "{" + "1:delay" + "}" + ", " + "$" + "{" + "2:increment" + "}" + ")"),
        boost: 10
    },                {
        label: "delete",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">delete</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DeleteFromStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a new DELETE query builder.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\delete()"),
        boost: 10
    },                {
        label: "dense_rank",
        type: "function",
        detail: "flow\u002Ddsl\u002Dwindow\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dense_rank</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DenseRank</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\dense_rank()"),
        boost: 10
    },                {
        label: "dens_rank",
        type: "function",
        detail: "flow\u002Ddsl\u002Dwindow\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dens_rank</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DenseRank</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\dens_rank()"),
        boost: 10
    },                {
        label: "derived",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">derived</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SelectFinalStep</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$alias</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DerivedTable</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a derived table (subquery in FROM clause).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\derived(" + "$" + "{" + "1:query" + "}" + ", " + "$" + "{" + "2:alias" + "}" + ")"),
        boost: 10
    },                {
        label: "desc",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">desc</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">NullsPosition</span> <span class=\"fn-param\">$nulls</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\PostgreSql\\QueryBuilder\\Clause\\NullsPosition::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OrderBy</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an ORDER BY item with DESC direction.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\desc(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:nulls" + "}" + ")"),
        boost: 10
    },                {
        label: "df",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">df</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Config|ConfigBuilder|null</span> <span class=\"fn-param\">$config</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Flow</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for data_frame() : Flow.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\df(" + "$" + "{" + "1:config" + "}" + ")"),
        boost: 10
    },                {
        label: "discard",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">discard</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DiscardType</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DiscardFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a DISCARD builder.<br>Example: discard(DiscardType::ALL)<br>Produces: DISCARD ALL<br>@param DiscardType $type Type of resources to discard (ALL, PLANS, SEQUENCES, TEMP)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\discard(" + "$" + "{" + "1:type" + "}" + ")"),
        boost: 10
    },                {
        label: "dom_element_to_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dom_element_to_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DOMElement</span> <span class=\"fn-param\">$element</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$format_output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$preserver_white_space</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string|false</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @deprecated Please use \\Flow\\Types\\DSL\\dom_element_to_string() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\dom_element_to_string(" + "$" + "{" + "1:element" + "}" + ", " + "$" + "{" + "2:format_output" + "}" + ", " + "$" + "{" + "3:preserver_white_space" + "}" + ")"),
        boost: 10
    },                {
        label: "dom_element_to_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dom_element_to_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DOMElement</span> <span class=\"fn-param\">$element</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$format_output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$preserver_white_space</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string|false</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\dom_element_to_string(" + "$" + "{" + "1:element" + "}" + ", " + "$" + "{" + "2:format_output" + "}" + ", " + "$" + "{" + "3:preserver_white_space" + "}" + ")"),
        boost: 10
    },                {
        label: "do_block",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">do_block</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$code</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DoFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Creates a DO statement builder for executing an anonymous code block.<br>Example: do_block(\'BEGIN RAISE NOTICE $$Hello World$$; END;\')<br>Produces: DO $$ BEGIN RAISE NOTICE $$Hello World$$; END; $$ LANGUAGE plpgsql<br>Example: do_block(\'SELECT 1\')->language(\'sql\')<br>Produces: DO $$ SELECT 1 $$ LANGUAGE sql<br>@param string $code The anonymous code block to execute<br>@return DoFinalStep Builder for DO statement options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\do_block(" + "$" + "{" + "1:code" + "}" + ")"),
        boost: 10
    },                {
        label: "drop",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">drop</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Drop</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\drop(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },                {
        label: "drop",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">drop</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DropFactory</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a factory for building DROP statements.<br>Provides a unified entry point for all DROP operations:<br>- drop()->table() - DROP TABLE<br>- drop()->index() - DROP INDEX<br>- drop()->view() - DROP VIEW<br>- drop()->materializedView() - DROP MATERIALIZED VIEW<br>- drop()->sequence() - DROP SEQUENCE<br>- drop()->schema() - DROP SCHEMA<br>- drop()->role() - DROP ROLE<br>- drop()->function() - DROP FUNCTION<br>- drop()->procedure() - DROP PROCEDURE<br>- drop()->trigger() - DROP TRIGGER<br>- drop()->rule() - DROP RULE<br>- drop()->extension() - DROP EXTENSION<br>- drop()->type() - DROP TYPE<br>- drop()->domain() - DROP DOMAIN<br>- drop()->owned() - DROP OWNED<br>Example: drop()->table(\'users\', \'orders\')->ifExists()->cascade()<br>Example: drop()->index(\'idx_email\')->ifExists()
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\drop()"),
        boost: 10
    },                {
        label: "drop_owned",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">drop_owned</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$roles</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DropOwnedFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a DROP OWNED builder.<br>Example: drop_owned(\'role1\')<br>Produces: DROP OWNED BY role1<br>Example: drop_owned(\'role1\', \'role2\')->cascade()<br>Produces: DROP OWNED BY role1, role2 CASCADE<br>@param string ...$roles The roles whose owned objects should be dropped<br>@return DropOwnedFinalStep Builder for drop owned options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\drop_owned(" + "$" + "{" + "1:roles" + "}" + ")"),
        boost: 10
    },                {
        label: "duration_microseconds",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">duration_microseconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$microseconds</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Duration</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\duration_microseconds(" + "$" + "{" + "1:microseconds" + "}" + ")"),
        boost: 10
    },                {
        label: "duration_milliseconds",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">duration_milliseconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$milliseconds</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Duration</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\duration_milliseconds(" + "$" + "{" + "1:milliseconds" + "}" + ")"),
        boost: 10
    },                {
        label: "duration_minutes",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">duration_minutes</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$minutes</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Duration</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\duration_minutes(" + "$" + "{" + "1:minutes" + "}" + ")"),
        boost: 10
    },                {
        label: "duration_seconds",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">duration_seconds</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$seconds</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Duration</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\duration_seconds(" + "$" + "{" + "1:seconds" + "}" + ")"),
        boost: 10
    },                {
        label: "empty_generator",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">empty_generator</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Generator</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Parquet\\empty_generator()"),
        boost: 10
    },                {
        label: "entries",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">entries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Entry</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entries</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Entry<mixed> ...$entries
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\entries(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },                {
        label: "entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EntryReference</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    An alias for \`ref\`.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\entry(" + "$" + "{" + "1:entry" + "}" + ")"),
        boost: 10
    },                {
        label: "entry_id_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">entry_id_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IdFactory</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Elasticsearch\\entry_id_factory(" + "$" + "{" + "1:entry_name" + "}" + ")"),
        boost: 10
    },                {
        label: "enum_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">enum_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">UnitEnum</span> <span class=\"fn-param\">$enum</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\UnitEnum>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\enum_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:enum" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "enum_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">enum_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EnumDefinition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T of \\UnitEnum<br>@param class-string<T> $type<br>@return EnumDefinition<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\enum_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:type" + "}" + ", " + "$" + "{" + "3:nullable" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "environment_detector",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">environment_detector</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EnvironmentDetector</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an EnvironmentDetector.<br>Detects resource attributes from OpenTelemetry standard environment variables:<br>- OTEL_SERVICE_NAME: Sets service.name attribute<br>- OTEL_RESOURCE_ATTRIBUTES: Sets additional attributes in key=value,key2=value2 format
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\environment_detector()"),
        boost: 10
    },                {
        label: "eq",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">eq</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparison</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an equality comparison (column = value).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\eq(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "equal",
        type: "function",
        detail: "flow\u002Ddsl\u002Dcomparisons",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">equal</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Equal</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\equal(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "es_hits_to_rows",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">es_hits_to_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DocumentDataSource</span> <span class=\"fn-param\">$source</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Adapter\\Elasticsearch\\ElasticsearchPHP\\DocumentDataSource::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HitsIntoRowsTransformer</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Transforms elasticsearch results into clear Flow Rows using [\'hits\'][\'hits\'][x][\'_source\'].<br>@return HitsIntoRowsTransformer
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Elasticsearch\\es_hits_to_rows(" + "$" + "{" + "1:source" + "}" + ")"),
        boost: 10
    },                {
        label: "exception_if_exists",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">exception_if_exists</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for save_mode_exception_if_exists().
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\exception_if_exists()"),
        boost: 10
    },                {
        label: "execution_context",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">execution_context</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Config</span> <span class=\"fn-param\">$config</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FlowContext</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\execution_context(" + "$" + "{" + "1:config" + "}" + ")"),
        boost: 10
    },                {
        label: "execution_lenient",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">execution_lenient</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ExecutionMode</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    In this mode, functions returns nulls instead of throwing exceptions.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\execution_lenient()"),
        boost: 10
    },                {
        label: "execution_strict",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">execution_strict</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ExecutionMode</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    In this mode, functions throws exceptions if the given entry is not found<br>or passed parameters are invalid.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\execution_strict()"),
        boost: 10
    },                {
        label: "exists",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">exists</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Exists</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\exists(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },                {
        label: "exists",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">exists</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SelectFinalStep</span> <span class=\"fn-param\">$subquery</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Exists</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an EXISTS condition.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\exists(" + "$" + "{" + "1:subquery" + "}" + ")"),
        boost: 10
    },                {
        label: "explain",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">explain</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SelectFinalStep|InsertBuilder|UpdateBuilder|DeleteBuilder</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ExplainFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an EXPLAIN builder for a query.<br>Example: explain(select()->from(\'users\'))<br>Produces: EXPLAIN SELECT * FROM users<br>@param DeleteBuilder|InsertBuilder|SelectFinalStep|UpdateBuilder $query Query to explain
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\explain(" + "$" + "{" + "1:query" + "}" + ")"),
        boost: 10
    },                {
        label: "fetch",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">fetch</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$cursorName</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FetchCursorBuilder</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Fetch rows from a cursor.<br>Example: fetch(\'my_cursor\')->forward(100)<br>Produces: FETCH FORWARD 100 my_cursor<br>Example: fetch(\'my_cursor\')->all()<br>Produces: FETCH ALL my_cursor<br>@param string $cursorName Cursor to fetch from
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\fetch(" + "$" + "{" + "1:cursorName" + "}" + ")"),
        boost: 10
    },                {
        label: "files",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">files</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$directory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FilesExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\files(" + "$" + "{" + "1:directory" + "}" + ")"),
        boost: 10
    },                {
        label: "filesystem_cache",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">filesystem_cache</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string|null</span> <span class=\"fn-param\">$cache_dir</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Filesystem</span> <span class=\"fn-param\">$filesystem</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Filesystem\\Local\\NativeLocalFilesystem::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Serializer</span> <span class=\"fn-param\">$serializer</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Serializer\\NativePHPSerializer::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FilesystemCache</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\filesystem_cache(" + "$" + "{" + "1:cache_dir" + "}" + ", " + "$" + "{" + "2:filesystem" + "}" + ", " + "$" + "{" + "3:serializer" + "}" + ")"),
        boost: 10
    },                {
        label: "filesystem_telemetry_config",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">filesystem_telemetry_config</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Telemetry</span> <span class=\"fn-param\">$telemetry</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ClockInterface</span> <span class=\"fn-param\">$clock</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">FilesystemTelemetryOptions</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FilesystemTelemetryConfig</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a telemetry configuration for the filesystem.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\filesystem_telemetry_config(" + "$" + "{" + "1:telemetry" + "}" + ", " + "$" + "{" + "2:clock" + "}" + ", " + "$" + "{" + "3:options" + "}" + ")"),
        boost: 10
    },                {
        label: "filesystem_telemetry_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">filesystem_telemetry_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$traceStreams</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$collectMetrics</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FilesystemTelemetryOptions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create options for filesystem telemetry.<br>@param bool $traceStreams Create a single span per stream lifecycle (default: ON)<br>@param bool $collectMetrics Collect metrics for bytes/operation counts (default: ON)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\filesystem_telemetry_options(" + "$" + "{" + "1:traceStreams" + "}" + ", " + "$" + "{" + "2:collectMetrics" + "}" + ")"),
        boost: 10
    },                {
        label: "first",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">first</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">First</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\first(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },                {
        label: "float_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">float_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string|int|float|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?float>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\float_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "float_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">float_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FloatDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\float_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "flow_context",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">flow_context</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Config</span> <span class=\"fn-param\">$config</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FlowContext</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\flow_context(" + "$" + "{" + "1:config" + "}" + ")"),
        boost: 10
    },                {
        label: "foreign_key",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">foreign_key</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$columns</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$referenceTable</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$referenceColumns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ForeignKeyConstraint</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a FOREIGN KEY constraint.<br>@param list<string> $columns Local columns<br>@param string $referenceTable Referenced table<br>@param list<string> $referenceColumns Referenced columns (defaults to same as $columns if empty)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\foreign_key(" + "$" + "{" + "1:columns" + "}" + ", " + "$" + "{" + "2:referenceTable" + "}" + ", " + "$" + "{" + "3:referenceColumns" + "}" + ")"),
        boost: 10
    },                {
        label: "for_share",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">for_share</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$tables</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LockingClause</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a FOR SHARE locking clause.<br>@param list<string> $tables Tables to lock (empty for all)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\for_share(" + "$" + "{" + "1:tables" + "}" + ")"),
        boost: 10
    },                {
        label: "for_update",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">for_update</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$tables</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LockingClause</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a FOR UPDATE locking clause.<br>@param list<string> $tables Tables to lock (empty for all)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\for_update(" + "$" + "{" + "1:tables" + "}" + ")"),
        boost: 10
    },                {
        label: "frame_current_row",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">frame_current_row</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FrameBound</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a frame bound for CURRENT ROW.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\frame_current_row()"),
        boost: 10
    },                {
        label: "frame_following",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">frame_following</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$offset</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FrameBound</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a frame bound for N FOLLOWING.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\frame_following(" + "$" + "{" + "1:offset" + "}" + ")"),
        boost: 10
    },                {
        label: "frame_preceding",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">frame_preceding</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$offset</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FrameBound</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a frame bound for N PRECEDING.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\frame_preceding(" + "$" + "{" + "1:offset" + "}" + ")"),
        boost: 10
    },                {
        label: "frame_unbounded_following",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">frame_unbounded_following</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FrameBound</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a frame bound for UNBOUNDED FOLLOWING.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\frame_unbounded_following()"),
        boost: 10
    },                {
        label: "frame_unbounded_preceding",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">frame_unbounded_preceding</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FrameBound</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a frame bound for UNBOUNDED PRECEDING.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\frame_unbounded_preceding()"),
        boost: 10
    },                {
        label: "from_all",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$extractors</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChainExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_all(" + "$" + "{" + "1:extractors" + "}" + ")"),
        boost: 10
    },                {
        label: "from_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_array</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">iterable</span> <span class=\"fn-param\">$array</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param iterable<array<mixed>> $array<br>@param null|Schema $schema - @deprecated use withSchema() method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_array(" + "$" + "{" + "1:array" + "}" + ", " + "$" + "{" + "2:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "from_avro",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_avro</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AvroExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\Adapter\\Avro\\from_avro(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },                {
        label: "from_cache",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_cache</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$id</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Extractor</span> <span class=\"fn-param\">$fallback_extractor</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$clear</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CacheExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param string $id - cache id from which data will be extracted<br>@param null|Extractor $fallback_extractor - extractor that will be used when cache is empty - @deprecated use withFallbackExtractor() method instead<br>@param bool $clear - clear cache after extraction - @deprecated use withClearOnFinish() method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_cache(" + "$" + "{" + "1:id" + "}" + ", " + "$" + "{" + "2:fallback_extractor" + "}" + ", " + "$" + "{" + "3:clear" + "}" + ")"),
        boost: 10
    },                {
        label: "from_csv",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_csv</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$with_header</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$empty_to_null</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$enclosure</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$escape</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$characters_read_in_line</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1000</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CSVExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path<br>@param bool $empty_to_null - @deprecated use $loader->withEmptyToNull() instead<br>@param bool $with_header - @deprecated use $loader->withHeader() instead<br>@param null|string $separator - @deprecated use $loader->withSeparator() instead<br>@param null|string $enclosure - @deprecated use $loader->withEnclosure() instead<br>@param null|string $escape - @deprecated use $loader->withEscape() instead<br>@param int<1, max> $characters_read_in_line - @deprecated use $loader->withCharactersReadInLine() instead<br>@param null|Schema $schema - @deprecated use $loader->withSchema() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\CSV\\from_csv(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:with_header" + "}" + ", " + "$" + "{" + "3:empty_to_null" + "}" + ", " + "$" + "{" + "4:separator" + "}" + ", " + "$" + "{" + "5:enclosure" + "}" + ", " + "$" + "{" + "6:escape" + "}" + ", " + "$" + "{" + "7:characters_read_in_line" + "}" + ", " + "$" + "{" + "8:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "from_data_frame",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_data_frame</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DataFrame</span> <span class=\"fn-param\">$data_frame</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DataFrameExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_data_frame(" + "$" + "{" + "1:data_frame" + "}" + ")"),
        boost: 10
    },                {
        label: "from_dbal_key_set_qb",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_dbal_key_set_qb</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">QueryBuilder</span> <span class=\"fn-param\">$queryBuilder</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">KeySet</span> <span class=\"fn-param\">$key_set</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalKeySetExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_key_set_qb(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:queryBuilder" + "}" + ", " + "$" + "{" + "3:key_set" + "}" + ")"),
        boost: 10
    },                {
        label: "from_dbal_limit_offset",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_dbal_limit_offset</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Table|string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">OrderBy|array</span> <span class=\"fn-param\">$order_by</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$page_size</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1000</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$maximum</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLimitOffsetExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Connection $connection<br>@param string|Table $table<br>@param array<OrderBy>|OrderBy $order_by<br>@param int $page_size<br>@param null|int $maximum<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_limit_offset(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:table" + "}" + ", " + "$" + "{" + "3:order_by" + "}" + ", " + "$" + "{" + "4:page_size" + "}" + ", " + "$" + "{" + "5:maximum" + "}" + ")"),
        boost: 10
    },                {
        label: "from_dbal_limit_offset_qb",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_dbal_limit_offset_qb</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">QueryBuilder</span> <span class=\"fn-param\">$queryBuilder</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$page_size</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1000</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$maximum</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLimitOffsetExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Connection $connection<br>@param int $page_size<br>@param null|int $maximum - maximum can also be taken from a query builder, $maximum however is used regardless of the query builder if it\'s set<br>@param int $offset - offset can also be taken from a query builder, $offset however is used regardless of the query builder if it\'s set to non 0 value
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_limit_offset_qb(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:queryBuilder" + "}" + ", " + "$" + "{" + "3:page_size" + "}" + ", " + "$" + "{" + "4:maximum" + "}" + ", " + "$" + "{" + "5:offset" + "}" + ")"),
        boost: 10
    },                {
        label: "from_dbal_queries",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_dbal_queries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ParametersSet</span> <span class=\"fn-param\">$parameters_set</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalQueryExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param null|ParametersSet $parameters_set - each one parameters array will be evaluated as new query<br>@param array<int|string, DbalArrayType|DbalParameterType|DbalType|int|string> $types
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_queries(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:query" + "}" + ", " + "$" + "{" + "3:parameters_set" + "}" + ", " + "$" + "{" + "4:types" + "}" + ")"),
        boost: 10
    },                {
        label: "from_dbal_query",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_dbal_query</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalQueryExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string, mixed>|list<mixed> $parameters - @deprecated use DbalQueryExtractor::withParameters() instead<br>@param array<int<0, max>|string, DbalArrayType|DbalParameterType|DbalType|string> $types - @deprecated use DbalQueryExtractor::withTypes() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\from_dbal_query(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:query" + "}" + ", " + "$" + "{" + "3:parameters" + "}" + ", " + "$" + "{" + "4:types" + "}" + ")"),
        boost: 10
    },                {
        label: "from_dynamic_http_requests",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_dynamic_http_requests</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ClientInterface</span> <span class=\"fn-param\">$client</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">NextRequestFactory</span> <span class=\"fn-param\">$requestFactory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PsrHttpClientDynamicExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Http\\from_dynamic_http_requests(" + "$" + "{" + "1:client" + "}" + ", " + "$" + "{" + "2:requestFactory" + "}" + ")"),
        boost: 10
    },                {
        label: "from_es",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_es</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$pit_params</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ElasticsearchExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Extractor will automatically try to iterate over whole index using one of the two iteration methods:.<br>- from/size<br>- search_after<br>Search after is selected when you provide define sort parameters in query, otherwise it will fallback to from/size.<br>@param array{<br> hosts?: array<string>,<br> connectionParams?: array<mixed>,<br> retries?: int,<br> sniffOnStart?: bool,<br> sslCert?: array<string>,<br> sslKey?: array<string>,<br> sslVerification?: bool|string,<br> elasticMetaHeader?: bool,<br> includePortInHostHeader?: bool<br>} $config<br>@param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/search-search.html<br>@param ?array<mixed> $pit_params - when used extractor will create point in time to stabilize search results. Point in time is automatically closed when last element is extracted. https://www.elastic.co/guide/en/elasticsearch/reference/master/point-in-time-api.html - @deprecated use withPointInTime method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Elasticsearch\\from_es(" + "$" + "{" + "1:config" + "}" + ", " + "$" + "{" + "2:parameters" + "}" + ", " + "$" + "{" + "3:pit_params" + "}" + ")"),
        boost: 10
    },                {
        label: "from_excel",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_excel</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ExcelExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Excel\\DSL\\from_excel(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },                {
        label: "from_google_sheet",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_google_sheet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Sheets|array</span> <span class=\"fn-param\">$auth_config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$spreadsheet_id</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$sheet_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$with_header</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$rows_per_page</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1000</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">GoogleSheetExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config<br>@param string $spreadsheet_id<br>@param string $sheet_name<br>@param bool $with_header - @deprecated use withHeader method instead<br>@param int $rows_per_page - how many rows per page to fetch from Google Sheets API - @deprecated use withRowsPerPage method instead<br>@param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options - @deprecated use withOptions method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\GoogleSheet\\from_google_sheet(" + "$" + "{" + "1:auth_config" + "}" + ", " + "$" + "{" + "2:spreadsheet_id" + "}" + ", " + "$" + "{" + "3:sheet_name" + "}" + ", " + "$" + "{" + "4:with_header" + "}" + ", " + "$" + "{" + "5:rows_per_page" + "}" + ", " + "$" + "{" + "6:options" + "}" + ")"),
        boost: 10
    },                {
        label: "from_google_sheet_columns",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_google_sheet_columns</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Sheets|array</span> <span class=\"fn-param\">$auth_config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$spreadsheet_id</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$sheet_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$start_range_column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$end_range_column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$with_header</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$rows_per_page</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1000</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">GoogleSheetExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config<br>@param string $spreadsheet_id<br>@param string $sheet_name<br>@param string $start_range_column<br>@param string $end_range_column<br>@param bool $with_header - @deprecated use withHeader method instead<br>@param int $rows_per_page - how many rows per page to fetch from Google Sheets API, default 1000 - @deprecated use withRowsPerPage method instead<br>@param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options - @deprecated use withOptions method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\GoogleSheet\\from_google_sheet_columns(" + "$" + "{" + "1:auth_config" + "}" + ", " + "$" + "{" + "2:spreadsheet_id" + "}" + ", " + "$" + "{" + "3:sheet_name" + "}" + ", " + "$" + "{" + "4:start_range_column" + "}" + ", " + "$" + "{" + "5:end_range_column" + "}" + ", " + "$" + "{" + "6:with_header" + "}" + ", " + "$" + "{" + "7:rows_per_page" + "}" + ", " + "$" + "{" + "8:options" + "}" + ")"),
        boost: 10
    },                {
        label: "from_json",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_json</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$pointer</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path - string is internally turned into stream<br>@param ?string $pointer - if you want to iterate only results of a subtree, use a pointer, read more at https://github.com/halaxa/json-machine#parsing-a-subtree - @deprecate use withPointer method instead<br>@param null|Schema $schema - enforce schema on the extracted data - @deprecate use withSchema method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\JSON\\from_json(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:pointer" + "}" + ", " + "$" + "{" + "3:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "from_json_lines",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_json_lines</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonLinesExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Used to read from a JSON lines https://jsonlines.org/ formatted file.<br>@param Path|string $path - string is internally turned into stream
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\JSON\\from_json_lines(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },                {
        label: "from_memory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_memory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Memory</span> <span class=\"fn-param\">$memory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_memory(" + "$" + "{" + "1:memory" + "}" + ")"),
        boost: 10
    },                {
        label: "from_parquet",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_parquet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Parquet\\Options::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ByteOrder</span> <span class=\"fn-param\">$byte_order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Parquet\\ByteOrder::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ParquetExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path<br>@param array<string> $columns - list of columns to read from parquet file - @deprecated use \`withColumns\` method instead<br>@param Options $options - @deprecated use \`withOptions\` method instead<br>@param ByteOrder $byte_order - @deprecated use \`withByteOrder\` method instead<br>@param null|int $offset - @deprecated use \`withOffset\` method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Parquet\\from_parquet(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:columns" + "}" + ", " + "$" + "{" + "3:options" + "}" + ", " + "$" + "{" + "4:byte_order" + "}" + ", " + "$" + "{" + "5:offset" + "}" + ")"),
        boost: 10
    },                {
        label: "from_path_partitions",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_path_partitions</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PathPartitionsExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_path_partitions(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },                {
        label: "from_pipeline",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_pipeline</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Pipeline</span> <span class=\"fn-param\">$pipeline</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PipelineExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_pipeline(" + "$" + "{" + "1:pipeline" + "}" + ")"),
        boost: 10
    },                {
        label: "from_rows",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Rows</span> <span class=\"fn-param\">$rows</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RowsExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_rows(" + "$" + "{" + "1:rows" + "}" + ")"),
        boost: 10
    },                {
        label: "from_sequence_date_period",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_sequence_date_period</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface</span> <span class=\"fn-param\">$start</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface</span> <span class=\"fn-param\">$end</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SequenceExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_sequence_date_period(" + "$" + "{" + "1:entry_name" + "}" + ", " + "$" + "{" + "2:start" + "}" + ", " + "$" + "{" + "3:interval" + "}" + ", " + "$" + "{" + "4:end" + "}" + ", " + "$" + "{" + "5:options" + "}" + ")"),
        boost: 10
    },                {
        label: "from_sequence_date_period_recurrences",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_sequence_date_period_recurrences</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeInterface</span> <span class=\"fn-param\">$start</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateInterval</span> <span class=\"fn-param\">$interval</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$recurrences</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SequenceExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_sequence_date_period_recurrences(" + "$" + "{" + "1:entry_name" + "}" + ", " + "$" + "{" + "2:start" + "}" + ", " + "$" + "{" + "3:interval" + "}" + ", " + "$" + "{" + "4:recurrences" + "}" + ", " + "$" + "{" + "5:options" + "}" + ")"),
        boost: 10
    },                {
        label: "from_sequence_number",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_sequence_number</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string|int|float</span> <span class=\"fn-param\">$start</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string|int|float</span> <span class=\"fn-param\">$end</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int|float</span> <span class=\"fn-param\">$step</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SequenceExtractor</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\from_sequence_number(" + "$" + "{" + "1:entry_name" + "}" + ", " + "$" + "{" + "2:start" + "}" + ", " + "$" + "{" + "3:end" + "}" + ", " + "$" + "{" + "4:step" + "}" + ")"),
        boost: 10
    },                {
        label: "from_static_http_requests",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_static_http_requests</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ClientInterface</span> <span class=\"fn-param\">$client</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">iterable</span> <span class=\"fn-param\">$requests</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PsrHttpClientStaticExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param iterable<RequestInterface> $requests
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Http\\from_static_http_requests(" + "$" + "{" + "1:client" + "}" + ", " + "$" + "{" + "2:requests" + "}" + ")"),
        boost: 10
    },                {
        label: "from_text",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_text</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TextExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Text\\from_text(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },                {
        label: "from_xml",
        type: "function",
        detail: "flow\u002Ddsl\u002Dextractors",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">from_xml</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$xml_node_path</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">XMLParserExtractor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                     In order to iterate only over <element> nodes use \`from_xml($file)->withXMLNodePath(\'root/elements/element\')\`.<br> <root><br>   <elements><br>     <element></element><br>     <element></element><br>   <elements><br> </root><br> XML Node Path does not support attributes and it\'s not xpath, it is just a sequence<br> of node names separated with slash.<br>@param Path|string $path<br>@param string $xml_node_path - @deprecated use \`from_xml($file)->withXMLNodePath($xmlNodePath)\` method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\XML\\from_xml(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:xml_node_path" + "}" + ")"),
        boost: 10
    },                {
        label: "fstab",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">fstab</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Filesystem</span> <span class=\"fn-param\">$filesystems</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FilesystemTable</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a new filesystem table with given filesystems.<br>Filesystems can be also mounted later.<br>If no filesystems are provided, local filesystem is mounted.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\fstab(" + "$" + "{" + "1:filesystems" + "}" + ")"),
        boost: 10
    },                {
        label: "func",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">func</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$args</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FunctionCall</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a function call expression.<br>@param string $name Function name (can include schema like \"pg_catalog.now\")<br>@param list<Expression> $args Function arguments
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\func(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:args" + "}" + ")"),
        boost: 10
    },                {
        label: "func_arg",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">func_arg</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DataType</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">FunctionArgument</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Creates a new function argument for use in function/procedure definitions.<br>Example: func_arg(data_type_integer())<br>Example: func_arg(data_type_text())->named(\'username\')<br>Example: func_arg(data_type_integer())->named(\'count\')->default(\'0\')<br>Example: func_arg(data_type_text())->out()<br>@param DataType $type The PostgreSQL data type for the argument<br>@return FunctionArgument Builder for function argument options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\func_arg(" + "$" + "{" + "1:type" + "}" + ")"),
        boost: 10
    },                {
        label: "generate_random_int",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">generate_random_int</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$start</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">-9223372036854775808</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$end</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">9223372036854775807</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">NativePHPRandomValueGenerator</span> <span class=\"fn-param\">$generator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\NativePHPRandomValueGenerator::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\generate_random_int(" + "$" + "{" + "1:start" + "}" + ", " + "$" + "{" + "2:end" + "}" + ", " + "$" + "{" + "3:generator" + "}" + ")"),
        boost: 10
    },                {
        label: "generate_random_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">generate_random_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$length</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">32</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">NativePHPRandomValueGenerator</span> <span class=\"fn-param\">$generator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\NativePHPRandomValueGenerator::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\generate_random_string(" + "$" + "{" + "1:length" + "}" + ", " + "$" + "{" + "2:generator" + "}" + ")"),
        boost: 10
    },                {
        label: "get_type",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">get_type</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<mixed><br>@deprecated Please use \\Flow\\Types\\DSL\\get_type($value) instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\get_type(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },                {
        label: "get_type",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">get_type</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<mixed>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\get_type(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },                {
        label: "grant",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">grant</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">TablePrivilege|string</span> <span class=\"fn-param\">$privileges</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">GrantOnStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a GRANT privileges builder.<br>Example: grant(TablePrivilege::SELECT)->onTable(\'users\')->to(\'app_user\')<br>Produces: GRANT SELECT ON users TO app_user<br>Example: grant(TablePrivilege::ALL)->onAllTablesInSchema(\'public\')->to(\'admin\')<br>Produces: GRANT ALL ON ALL TABLES IN SCHEMA public TO admin<br>@param string|TablePrivilege ...$privileges The privileges to grant<br>@return GrantOnStep Builder for grant options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\grant(" + "$" + "{" + "1:privileges" + "}" + ")"),
        boost: 10
    },                {
        label: "grant_role",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">grant_role</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$roles</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">GrantRoleToStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a GRANT role builder.<br>Example: grant_role(\'admin\')->to(\'user1\')<br>Produces: GRANT admin TO user1<br>Example: grant_role(\'admin\', \'developer\')->to(\'user1\')->withAdminOption()<br>Produces: GRANT admin, developer TO user1 WITH ADMIN OPTION<br>@param string ...$roles The roles to grant<br>@return GrantRoleToStep Builder for grant role options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\grant_role(" + "$" + "{" + "1:roles" + "}" + ")"),
        boost: 10
    },                {
        label: "greatest",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">greatest</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Greatest</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\greatest(" + "$" + "{" + "1:values" + "}" + ")"),
        boost: 10
    },                {
        label: "greatest",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">greatest</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expressions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Greatest</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a GREATEST expression.<br>@param Expression ...$expressions Expressions to compare
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\greatest(" + "$" + "{" + "1:expressions" + "}" + ")"),
        boost: 10
    },                {
        label: "gt",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">gt</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparison</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a greater-than comparison (column > value).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\gt(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "gte",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">gte</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparison</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a greater-than-or-equal comparison (column >= value).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\gte(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "hash",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">hash</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Algorithm</span> <span class=\"fn-param\">$algorithm</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Hash\\NativePHPHash::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Hash</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\hash(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:algorithm" + "}" + ")"),
        boost: 10
    },                {
        label: "hash_id_factory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">hash_id_factory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry_names</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IdFactory</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Elasticsearch\\hash_id_factory(" + "$" + "{" + "1:entry_names" + "}" + ")"),
        boost: 10
    },                {
        label: "host_detector",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">host_detector</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HostDetector</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a HostDetector.<br>Detects host information including host.name, host.arch, and host.id<br>(from /etc/machine-id on Linux or IOPlatformUUID on macOS).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\host_detector()"),
        boost: 10
    },                {
        label: "html_element_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">html_element_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Dom\\HTMLElement|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?HTMLElement>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\html_element_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "html_element_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">html_element_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HTMLElementDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\html_element_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "html_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">html_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Dom\\HTMLDocument|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?HTMLDocument>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\html_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "html_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">html_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HTMLDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\html_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "identical",
        type: "function",
        detail: "flow\u002Ddsl\u002Dcomparisons",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">identical</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Identical</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\identical(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "ignore",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ignore</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for save_mode_ignore().
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\ignore()"),
        boost: 10
    },                {
        label: "ignore_error_handler",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ignore_error_handler</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IgnoreError</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\ignore_error_handler()"),
        boost: 10
    },                {
        label: "index_col",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">index_col</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IndexColumn</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an index column specification.<br>Use chainable methods: ->asc(), ->desc(), ->nullsFirst(), ->nullsLast(), ->opclass(), ->collate()<br>Example: index_col(\'email\')->desc()->nullsLast()<br>@param string $name The column name
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\index_col(" + "$" + "{" + "1:name" + "}" + ")"),
        boost: 10
    },                {
        label: "index_expr",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">index_expr</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expression</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IndexColumn</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an index column specification from an expression.<br>Use chainable methods: ->asc(), ->desc(), ->nullsFirst(), ->nullsLast(), ->opclass(), ->collate()<br>Example: index_expr(fn_call(\'lower\', col(\'email\')))->desc()<br>@param Expression $expression The expression to index
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\index_expr(" + "$" + "{" + "1:expression" + "}" + ")"),
        boost: 10
    },                {
        label: "index_method_brin",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">index_method_brin</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IndexMethod</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Get the BRIN index method.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\index_method_brin()"),
        boost: 10
    },                {
        label: "index_method_btree",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">index_method_btree</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IndexMethod</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Get the BTREE index method.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\index_method_btree()"),
        boost: 10
    },                {
        label: "index_method_gin",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">index_method_gin</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IndexMethod</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Get the GIN index method.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\index_method_gin()"),
        boost: 10
    },                {
        label: "index_method_gist",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">index_method_gist</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IndexMethod</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Get the GIST index method.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\index_method_gist()"),
        boost: 10
    },                {
        label: "index_method_hash",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">index_method_hash</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IndexMethod</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Get the HASH index method.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\index_method_hash()"),
        boost: 10
    },                {
        label: "index_method_spgist",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">index_method_spgist</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IndexMethod</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Get the SPGIST index method.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\index_method_spgist()"),
        boost: 10
    },                {
        label: "insert",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">insert</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">InsertIntoStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a new INSERT query builder.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\insert()"),
        boost: 10
    },                {
        label: "instrumentation_scope",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">instrumentation_scope</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$version</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;unknown&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$schemaUrl</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Attributes</span> <span class=\"fn-param\">$attributes</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Telemetry\\Attributes::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">InstrumentationScope</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an InstrumentationScope.<br>@param string $name The instrumentation scope name<br>@param string $version The instrumentation scope version<br>@param null|string $schemaUrl Optional schema URL
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\instrumentation_scope(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:version" + "}" + ", " + "$" + "{" + "3:schemaUrl" + "}" + ", " + "$" + "{" + "4:attributes" + "}" + ")"),
        boost: 10
    },                {
        label: "integer_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">integer_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?int>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\integer_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "integer_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">integer_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IntegerDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\integer_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "int_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">int_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?int>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\int_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "int_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">int_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IntegerDefinition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for \`integer_schema\`.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\int_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "is_distinct_from",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">is_distinct_from</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$not</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IsDistinctFrom</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an IS DISTINCT FROM condition.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\is_distinct_from(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ", " + "$" + "{" + "3:not" + "}" + ")"),
        boost: 10
    },                {
        label: "is_in",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">is_in</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">In</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an IN condition.<br>@param Expression $expr Expression to check<br>@param list<Expression> $values List of values
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\is_in(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:values" + "}" + ")"),
        boost: 10
    },                {
        label: "is_null",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">is_null</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$not</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IsNull</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an IS NULL condition.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\is_null(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:not" + "}" + ")"),
        boost: 10
    },                {
        label: "is_type",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">is_type</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type|array</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string|Type<mixed>>|Type<mixed> $type<br>@param mixed $value
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\is_type(" + "$" + "{" + "1:type" + "}" + ", " + "$" + "{" + "2:value" + "}" + ")"),
        boost: 10
    },                {
        label: "is_valid_excel_sheet_name",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">is_valid_excel_sheet_name</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$sheet_name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IsValidExcelSheetName</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Excel\\DSL\\is_valid_excel_sheet_name(" + "$" + "{" + "1:sheet_name" + "}" + ")"),
        boost: 10
    },                {
        label: "join_on",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">join_on</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Comparison|array</span> <span class=\"fn-param\">$comparisons</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$join_prefix</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Expression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<\\Flow\\ETL\\Join\\Comparison|string>|Comparison $comparisons
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\join_on(" + "$" + "{" + "1:comparisons" + "}" + ", " + "$" + "{" + "2:join_prefix" + "}" + ")"),
        boost: 10
    },                {
        label: "json_contained_by",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_contained_by</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OperatorCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a JSONB is contained by condition (<@).<br>Example: json_contained_by(col(\'metadata\'), literal_json(\'{\"category\": \"electronics\", \"price\": 100}\'))<br>Produces: metadata <@ \'{\"category\": \"electronics\", \"price\": 100}\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\json_contained_by(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "json_contains",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_contains</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OperatorCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a JSONB contains condition (@>).<br>Example: json_contains(col(\'metadata\'), literal_json(\'{\"category\": \"electronics\"}\'))<br>Produces: metadata @> \'{\"category\": \"electronics\"}\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\json_contains(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "json_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Json|array|string|null</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param null|array<array-key, mixed>|Json|string $data<br>@return Entry<?Json>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\json_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:data" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "json_exists",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_exists</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$key</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OperatorCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a JSONB key exists condition (?).<br>Example: json_exists(col(\'metadata\'), literal_string(\'category\'))<br>Produces: metadata ? \'category\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\json_exists(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:key" + "}" + ")"),
        boost: 10
    },                {
        label: "json_exists_all",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_exists_all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OperatorCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a JSONB all keys exist condition (?&).<br>Example: json_exists_all(col(\'metadata\'), raw_expr(\"array[\'category\', \'name\']\"))<br>Produces: metadata ?& array[\'category\', \'name\']
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\json_exists_all(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:keys" + "}" + ")"),
        boost: 10
    },                {
        label: "json_exists_any",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_exists_any</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OperatorCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a JSONB any key exists condition (?|).<br>Example: json_exists_any(col(\'metadata\'), raw_expr(\"array[\'category\', \'name\']\"))<br>Produces: metadata ?| array[\'category\', \'name\']
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\json_exists_any(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:keys" + "}" + ")"),
        boost: 10
    },                {
        label: "json_get",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_get</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$key</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BinaryExpression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a JSON field access expression (->).<br>Returns JSON.<br>Example: json_get(col(\'metadata\'), literal_string(\'category\'))<br>Produces: metadata -> \'category\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\json_get(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:key" + "}" + ")"),
        boost: 10
    },                {
        label: "json_get_text",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_get_text</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$key</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BinaryExpression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a JSON field access expression (->>).<br>Returns text.<br>Example: json_get_text(col(\'metadata\'), literal_string(\'name\'))<br>Produces: metadata ->> \'name\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\json_get_text(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:key" + "}" + ")"),
        boost: 10
    },                {
        label: "json_object_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_object_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Json|array|string|null</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param null|array<array-key, mixed>|Json|string $data<br>@throws InvalidArgumentException<br>@return Entry<?Json>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\json_object_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:data" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "json_path",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_path</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BinaryExpression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a JSON path access expression (#>).<br>Returns JSON.<br>Example: json_path(col(\'metadata\'), literal_string(\'{category,name}\'))<br>Produces: metadata #> \'{category,name}\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\json_path(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:path" + "}" + ")"),
        boost: 10
    },                {
        label: "json_path_text",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_path_text</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BinaryExpression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a JSON path access expression (#>>).<br>Returns text.<br>Example: json_path_text(col(\'metadata\'), literal_string(\'{category,name}\'))<br>Produces: metadata #>> \'{category,name}\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\json_path_text(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:path" + "}" + ")"),
        boost: 10
    },                {
        label: "json_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">json_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\json_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "last",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">last</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Last</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\last(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },                {
        label: "lateral",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">lateral</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">TableReference</span> <span class=\"fn-param\">$reference</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Lateral</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a LATERAL subquery.<br>@param TableReference $reference The subquery or table function reference
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\lateral(" + "$" + "{" + "1:reference" + "}" + ")"),
        boost: 10
    },                {
        label: "least",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">least</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$values</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Least</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\least(" + "$" + "{" + "1:values" + "}" + ")"),
        boost: 10
    },                {
        label: "least",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">least</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expressions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Least</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a LEAST expression.<br>@param Expression ...$expressions Expressions to compare
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\least(" + "$" + "{" + "1:expressions" + "}" + ")"),
        boost: 10
    },                {
        label: "like",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">like</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$caseInsensitive</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Like</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a LIKE condition.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\like(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:pattern" + "}" + ", " + "$" + "{" + "3:caseInsensitive" + "}" + ")"),
        boost: 10
    },                {
        label: "limit",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">limit</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Limit</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\limit(" + "$" + "{" + "1:limit" + "}" + ")"),
        boost: 10
    },                {
        label: "line_chart",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">line_chart</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference</span> <span class=\"fn-param\">$label</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">References</span> <span class=\"fn-param\">$datasets</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LineChart</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\ChartJS\\line_chart(" + "$" + "{" + "1:label" + "}" + ", " + "$" + "{" + "2:datasets" + "}" + ")"),
        boost: 10
    },                {
        label: "list_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">list_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ListType</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param null|list<mixed> $value<br>@param ListType<T> $type<br>@return Entry<mixed>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\list_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:type" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "list_ref",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">list_ref</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ListFunctions</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\list_ref(" + "$" + "{" + "1:entry" + "}" + ")"),
        boost: 10
    },                {
        label: "list_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">list_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ListType|Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ListDefinition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param ListType<T>|Type<list<T>> $type<br>@return ListDefinition<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\list_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:type" + "}" + ", " + "$" + "{" + "3:nullable" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "lit",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">lit</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Literal</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\lit(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },                {
        label: "literal",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">literal</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string|int|float|bool|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Literal</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a literal value for use in queries.<br>Automatically detects the type and creates the appropriate literal:<br>- literal(\'hello\') creates a string literal<br>- literal(42) creates an integer literal<br>- literal(3.14) creates a float literal<br>- literal(true) creates a boolean literal<br>- literal(null) creates a NULL literal
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\literal(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },                {
        label: "lock_for",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">lock_for</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">LockStrength</span> <span class=\"fn-param\">$strength</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$tables</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">LockWaitPolicy</span> <span class=\"fn-param\">$waitPolicy</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\PostgreSql\\QueryBuilder\\Clause\\LockWaitPolicy::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LockingClause</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a locking clause (FOR UPDATE, FOR SHARE, etc.).<br>@param LockStrength $strength Lock strength<br>@param list<string> $tables Tables to lock (empty for all)<br>@param LockWaitPolicy $waitPolicy Wait policy
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\lock_for(" + "$" + "{" + "1:strength" + "}" + ", " + "$" + "{" + "2:tables" + "}" + ", " + "$" + "{" + "3:waitPolicy" + "}" + ")"),
        boost: 10
    },                {
        label: "lock_table",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">lock_table</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$tables</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LockFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a LOCK TABLE builder.<br>Example: lock_table(\'users\', \'orders\')->accessExclusive()<br>Produces: LOCK TABLE users, orders IN ACCESS EXCLUSIVE MODE
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\lock_table(" + "$" + "{" + "1:tables" + "}" + ")"),
        boost: 10
    },                {
        label: "logger_provider",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">logger_provider</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">LogProcessor</span> <span class=\"fn-param\">$processor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ClockInterface</span> <span class=\"fn-param\">$clock</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ContextStorage</span> <span class=\"fn-param\">$contextStorage</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">LogRecordLimits</span> <span class=\"fn-param\">$limits</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Telemetry\\Logger\\LogRecordLimits::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LoggerProvider</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a LoggerProvider.<br>Creates a provider that uses a LogProcessor for processing logs.<br>For void/disabled logging, pass void_processor().<br>For memory-based testing, pass memory_processor() with exporters.<br>@param LogProcessor $processor The processor for logs<br>@param ClockInterface $clock The clock for timestamps<br>@param ContextStorage $contextStorage Storage for span correlation<br>@param LogRecordLimits $limits Limits for log record attributes
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\logger_provider(" + "$" + "{" + "1:processor" + "}" + ", " + "$" + "{" + "2:clock" + "}" + ", " + "$" + "{" + "3:contextStorage" + "}" + ", " + "$" + "{" + "4:limits" + "}" + ")"),
        boost: 10
    },                {
        label: "log_record_converter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">log_record_converter</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SeverityMapper</span> <span class=\"fn-param\">$severityMapper</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ValueNormalizer</span> <span class=\"fn-param\">$valueNormalizer</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LogRecordConverter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a LogRecordConverter for converting Monolog LogRecord to Telemetry LogRecord.<br>The converter handles:<br>- Severity mapping from Monolog Level to Telemetry Severity<br>- Message body conversion<br>- Channel and level name as monolog.* attributes<br>- Context values as context.* attributes (Throwables use setException())<br>- Extra values as extra.* attributes<br>@param null|SeverityMapper $severityMapper Custom severity mapper (defaults to standard mapping)<br>@param null|ValueNormalizer $valueNormalizer Custom value normalizer (defaults to standard normalizer)<br>Example usage:<br>\`\`\`php<br>$converter = log_record_converter();<br>$telemetryRecord = $converter->convert($monologRecord);<br>\`\`\`<br>Example with custom mapper:<br>\`\`\`php<br>$converter = log_record_converter(<br>    severityMapper: severity_mapper([<br>        Level::Debug->value => Severity::TRACE,<br>    ])<br>);<br>\`\`\`
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Monolog\\Telemetry\\DSL\\log_record_converter(" + "$" + "{" + "1:severityMapper" + "}" + ", " + "$" + "{" + "2:valueNormalizer" + "}" + ")"),
        boost: 10
    },                {
        label: "log_record_limits",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">log_record_limits</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$attributeCountLimit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">128</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$attributeValueLengthLimit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LogRecordLimits</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create LogRecordLimits configuration.<br>LogRecordLimits controls the maximum amount of data a log record can collect,<br>preventing unbounded memory growth and ensuring reasonable log record sizes.<br>@param int $attributeCountLimit Maximum number of attributes per log record<br>@param null|int $attributeValueLengthLimit Maximum length for string attribute values (null = unlimited)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\log_record_limits(" + "$" + "{" + "1:attributeCountLimit" + "}" + ", " + "$" + "{" + "2:attributeValueLengthLimit" + "}" + ")"),
        boost: 10
    },                {
        label: "lower",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">lower</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToLower</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\lower(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },                {
        label: "lt",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">lt</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparison</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a less-than comparison (column < value).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\lt(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "lte",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">lte</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparison</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a less-than-or-equal comparison (column <= value).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\lte(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "manual_detector",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">manual_detector</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$attributes</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ManualDetector</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a ManualDetector.<br>Returns manually specified resource attributes. Use this when you need<br>to set attributes explicitly rather than detecting them automatically.<br>@param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes Resource attributes
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\manual_detector(" + "$" + "{" + "1:attributes" + "}" + ")"),
        boost: 10
    },                {
        label: "map_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">map_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">MapType</span> <span class=\"fn-param\">$mapType</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template TKey of array-key<br>@template TValue<br>@param ?array<array-key, mixed> $value<br>@param MapType<TKey, TValue> $mapType<br>@return Entry<?array<TKey, TValue>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\map_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:mapType" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "map_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">map_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">MapType|Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MapDefinition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template TKey of array-key<br>@template TValue<br>@param MapType<TKey, TValue>|Type<array<TKey, TValue>> $type<br>@return MapDefinition<TKey, TValue>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\map_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:type" + "}" + ", " + "$" + "{" + "3:nullable" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "mask_columns",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">mask_columns</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$mask</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;******&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MaskColumns</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<int, string> $columns
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\mask_columns(" + "$" + "{" + "1:columns" + "}" + ", " + "$" + "{" + "2:mask" + "}" + ")"),
        boost: 10
    },                {
        label: "match_cases",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">match_cases</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$cases</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$default</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MatchCases</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<MatchCondition> $cases
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\match_cases(" + "$" + "{" + "1:cases" + "}" + ", " + "$" + "{" + "2:default" + "}" + ")"),
        boost: 10
    },                {
        label: "match_condition",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">match_condition</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$condition</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$then</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MatchCondition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\match_condition(" + "$" + "{" + "1:condition" + "}" + ", " + "$" + "{" + "2:then" + "}" + ")"),
        boost: 10
    },                {
        label: "max",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">max</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Max</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\max(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },                {
        label: "memory_context_storage",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">memory_context_storage</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Context</span> <span class=\"fn-param\">$context</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryContextStorage</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a MemoryContextStorage.<br>In-memory context storage for storing and retrieving the current context.<br>A single instance should be shared across all providers within a request lifecycle.<br>@param null|Context $context Optional initial context
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\memory_context_storage(" + "$" + "{" + "1:context" + "}" + ")"),
        boost: 10
    },                {
        label: "memory_filesystem",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">memory_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryFilesystem</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a new memory filesystem and writes data to it in memory.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\memory_filesystem()"),
        boost: 10
    },                {
        label: "memory_log_exporter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">memory_log_exporter</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryLogExporter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a MemoryLogExporter.<br>Log exporter that stores data in memory.<br>Provides direct getter access to exported log entries.<br>Useful for testing and inspection without serialization.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\memory_log_exporter()"),
        boost: 10
    },                {
        label: "memory_log_processor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">memory_log_processor</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">LogExporter</span> <span class=\"fn-param\">$exporter</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryLogProcessor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a MemoryLogProcessor.<br>Log processor that stores log records in memory and exports via configured exporter.<br>Useful for testing.<br>@param LogExporter $exporter The exporter to send logs to
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\memory_log_processor(" + "$" + "{" + "1:exporter" + "}" + ")"),
        boost: 10
    },                {
        label: "memory_metric_exporter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">memory_metric_exporter</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryMetricExporter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a MemoryMetricExporter.<br>Metric exporter that stores data in memory.<br>Provides direct getter access to exported metrics.<br>Useful for testing and inspection without serialization.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\memory_metric_exporter()"),
        boost: 10
    },                {
        label: "memory_metric_processor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">memory_metric_processor</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">MetricExporter</span> <span class=\"fn-param\">$exporter</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryMetricProcessor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a MemoryMetricProcessor.<br>Metric processor that stores metrics in memory and exports via configured exporter.<br>Useful for testing.<br>@param MetricExporter $exporter The exporter to send metrics to
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\memory_metric_processor(" + "$" + "{" + "1:exporter" + "}" + ")"),
        boost: 10
    },                {
        label: "memory_span_exporter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">memory_span_exporter</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemorySpanExporter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a MemorySpanExporter.<br>Span exporter that stores data in memory.<br>Provides direct getter access to exported spans.<br>Useful for testing and inspection without serialization.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\memory_span_exporter()"),
        boost: 10
    },                {
        label: "memory_span_processor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">memory_span_processor</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SpanExporter</span> <span class=\"fn-param\">$exporter</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemorySpanProcessor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a MemorySpanProcessor.<br>Span processor that stores spans in memory and exports via configured exporter.<br>Useful for testing.<br>@param SpanExporter $exporter The exporter to send spans to
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\memory_span_processor(" + "$" + "{" + "1:exporter" + "}" + ")"),
        boost: 10
    },                {
        label: "merge",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">merge</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$alias</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MergeUsingStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a new MERGE query builder.<br>@param string $table Target table name<br>@param null|string $alias Optional table alias
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\merge(" + "$" + "{" + "1:table" + "}" + ", " + "$" + "{" + "2:alias" + "}" + ")"),
        boost: 10
    },                {
        label: "meter_provider",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">meter_provider</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">MetricProcessor</span> <span class=\"fn-param\">$processor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ClockInterface</span> <span class=\"fn-param\">$clock</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">AggregationTemporality</span> <span class=\"fn-param\">$temporality</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Telemetry\\Meter\\AggregationTemporality::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ExemplarFilter</span> <span class=\"fn-param\">$exemplarFilter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Telemetry\\Meter\\Exemplar\\TraceBasedExemplarFilter::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">MetricLimits</span> <span class=\"fn-param\">$limits</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Telemetry\\Meter\\MetricLimits::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MeterProvider</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a MeterProvider.<br>Creates a provider that uses a MetricProcessor for processing metrics.<br>For void/disabled metrics, pass void_processor().<br>For memory-based testing, pass memory_processor() with exporters.<br>@param MetricProcessor $processor The processor for metrics<br>@param ClockInterface $clock The clock for timestamps<br>@param AggregationTemporality $temporality Aggregation temporality for metrics<br>@param ExemplarFilter $exemplarFilter Filter for exemplar sampling (default: TraceBasedExemplarFilter)<br>@param MetricLimits $limits Cardinality limits for metric instruments
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\meter_provider(" + "$" + "{" + "1:processor" + "}" + ", " + "$" + "{" + "2:clock" + "}" + ", " + "$" + "{" + "3:temporality" + "}" + ", " + "$" + "{" + "4:exemplarFilter" + "}" + ", " + "$" + "{" + "5:limits" + "}" + ")"),
        boost: 10
    },                {
        label: "metric_limits",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">metric_limits</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$cardinalityLimit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">2000</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MetricLimits</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create MetricLimits configuration.<br>MetricLimits controls the maximum cardinality (unique attribute combinations)<br>per metric instrument, preventing memory exhaustion from high-cardinality attributes.<br>When the cardinality limit is exceeded, new attribute combinations are aggregated<br>into an overflow data point with \`otel.metric.overflow: true\` attribute.<br>Note: Unlike spans and logs, metrics are EXEMPT from attribute count and value<br>length limits per the OpenTelemetry specification. Only cardinality is limited.<br>@param int $cardinalityLimit Maximum number of unique attribute combinations per instrument
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\metric_limits(" + "$" + "{" + "1:cardinalityLimit" + "}" + ")"),
        boost: 10
    },                {
        label: "min",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">min</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Min</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\min(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },                {
        label: "mysql_insert_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">mysql_insert_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$skip_conflicts</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$upsert</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$update_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MySQLInsertOptions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string> $update_columns
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\mysql_insert_options(" + "$" + "{" + "1:skip_conflicts" + "}" + ", " + "$" + "{" + "2:upsert" + "}" + ", " + "$" + "{" + "3:update_columns" + "}" + ")"),
        boost: 10
    },                {
        label: "native_local_filesystem",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">native_local_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">NativeLocalFilesystem</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\native_local_filesystem()"),
        boost: 10
    },                {
        label: "neq",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">neq</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Comparison</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a not-equal comparison (column != value).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\neq(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "not",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">not</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Not</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\not(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },                {
        label: "not_regex_imatch",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">not_regex_imatch</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OperatorCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a POSIX regex not match condition (!~*).<br>Case-insensitive.<br>Example: not_regex_imatch(col(\'email\'), literal_string(\'.*@spam\\\\.com\'))<br>Produces: email !~* \'.*@spam\\.com\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\not_regex_imatch(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:pattern" + "}" + ")"),
        boost: 10
    },                {
        label: "not_regex_match",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">not_regex_match</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OperatorCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a POSIX regex not match condition (!~).<br>Case-sensitive.<br>Example: not_regex_match(col(\'email\'), literal_string(\'.*@spam\\\\.com\'))<br>Produces: email !~ \'.*@spam\\.com\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\not_regex_match(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:pattern" + "}" + ")"),
        boost: 10
    },                {
        label: "now",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">now</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DateTimeZone|ScalarFunction</span> <span class=\"fn-param\">$time_zone</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">DateTimeZone::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Now</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\now(" + "$" + "{" + "1:time_zone" + "}" + ")"),
        boost: 10
    },                {
        label: "nullif",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">nullif</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr1</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr2</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">NullIf</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a NULLIF expression.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\nullif(" + "$" + "{" + "1:expr1" + "}" + ", " + "$" + "{" + "2:expr2" + "}" + ")"),
        boost: 10
    },                {
        label: "null_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">null_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    This functions is an alias for creating string entry from null.<br>The main difference between using this function an simply str_entry with second argument null<br>is that this function will also keep a note in the metadata that type might not be final.<br>For example when we need to guess column type from rows because schema was not provided,<br>and given column in the first row is null, it might still change once we get to the second row.<br>That metadata is used to determine if string_entry was created from null or not.<br>By design flow assumes when guessing column type that null would be a string (the most flexible type).<br>@return Entry<?string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\null_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "null_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">null_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\null_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "number_format",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">number_format</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int|float</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$decimals</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">2</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$decimal_separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;.&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$thousands_separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;,&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">NumberFormat</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\number_format(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:decimals" + "}" + ", " + "$" + "{" + "3:decimal_separator" + "}" + ", " + "$" + "{" + "4:thousands_separator" + "}" + ")"),
        boost: 10
    },                {
        label: "on_conflict_nothing",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">on_conflict_nothing</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ConflictTarget</span> <span class=\"fn-param\">$target</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OnConflictClause</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an ON CONFLICT DO NOTHING clause.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\on_conflict_nothing(" + "$" + "{" + "1:target" + "}" + ")"),
        boost: 10
    },                {
        label: "on_conflict_update",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">on_conflict_update</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ConflictTarget</span> <span class=\"fn-param\">$target</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$updates</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OnConflictClause</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an ON CONFLICT DO UPDATE clause.<br>@param ConflictTarget $target Conflict target (columns or constraint)<br>@param array<string, Expression> $updates Column updates
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\on_conflict_update(" + "$" + "{" + "1:target" + "}" + ", " + "$" + "{" + "2:updates" + "}" + ")"),
        boost: 10
    },                {
        label: "optional",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">optional</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Optional</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\optional(" + "$" + "{" + "1:function" + "}" + ")"),
        boost: 10
    },                {
        label: "order_by",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">order_by</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SortDirection</span> <span class=\"fn-param\">$direction</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\PostgreSql\\QueryBuilder\\Clause\\SortDirection::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">NullsPosition</span> <span class=\"fn-param\">$nulls</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\PostgreSql\\QueryBuilder\\Clause\\NullsPosition::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OrderBy</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an ORDER BY item.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\order_by(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:direction" + "}" + ", " + "$" + "{" + "3:nulls" + "}" + ")"),
        boost: 10
    },                {
        label: "os_detector",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">os_detector</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OsDetector</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an OsDetector.<br>Detects operating system information including os.type, os.name, os.version,<br>and os.description using PHP\'s php_uname() function.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\os_detector()"),
        boost: 10
    },                {
        label: "otlp_curl_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">otlp_curl_options</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CurlTransportOptions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create curl transport options for OTLP.<br>Returns a CurlTransportOptions builder for configuring curl transport settings<br>using a fluent interface.<br>Example usage:<br>\`\`\`php<br>$options = otlp_curl_options()<br>    ->withTimeout(60)<br>    ->withConnectTimeout(15)<br>    ->withHeader(\'Authorization\', \'Bearer token\')<br>    ->withCompression()<br>    ->withSslVerification(verifyPeer: true);<br>$transport = otlp_curl_transport($endpoint, $serializer, $options);<br>\`\`\`
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Telemetry\\OTLP\\DSL\\otlp_curl_options()"),
        boost: 10
    },                {
        label: "otlp_curl_transport",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">otlp_curl_transport</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$endpoint</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Serializer</span> <span class=\"fn-param\">$serializer</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">CurlTransportOptions</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Bridge\\Telemetry\\OTLP\\Transport\\CurlTransportOptions::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CurlTransport</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an async curl transport for OTLP endpoints.<br>Creates a CurlTransport that uses curl_multi for non-blocking I/O.<br>Unlike HttpTransport (PSR-18), this transport queues requests and executes<br>them asynchronously. Completed requests are processed on subsequent send()<br>calls or on shutdown().<br>Requires: ext-curl PHP extension<br>Example usage:<br>\`\`\`php<br>// JSON over HTTP (async) with default options<br>$transport = otlp_curl_transport(<br>    endpoint: \'http://localhost:4318\',<br>    serializer: otlp_json_serializer(),<br>);<br>// Protobuf over HTTP (async) with custom options<br>$transport = otlp_curl_transport(<br>    endpoint: \'http://localhost:4318\',<br>    serializer: otlp_protobuf_serializer(),<br>    options: otlp_curl_options()<br>        ->withTimeout(60)<br>        ->withHeader(\'Authorization\', \'Bearer token\')<br>        ->withCompression(),<br>);<br>\`\`\`<br>@param string $endpoint OTLP endpoint URL (e.g., \'http://localhost:4318\')<br>@param Serializer $serializer Serializer for encoding telemetry data (JSON or Protobuf)<br>@param CurlTransportOptions $options Transport configuration options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Telemetry\\OTLP\\DSL\\otlp_curl_transport(" + "$" + "{" + "1:endpoint" + "}" + ", " + "$" + "{" + "2:serializer" + "}" + ", " + "$" + "{" + "3:options" + "}" + ")"),
        boost: 10
    },                {
        label: "otlp_grpc_transport",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">otlp_grpc_transport</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$endpoint</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ProtobufSerializer</span> <span class=\"fn-param\">$serializer</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$headers</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$insecure</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">GrpcTransport</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a gRPC transport for OTLP endpoints.<br>Creates a GrpcTransport configured to send telemetry data to an OTLP-compatible<br>endpoint using gRPC protocol with Protobuf serialization.<br>Requires:<br>- ext-grpc PHP extension<br>- google/protobuf package<br>- open-telemetry/gen-otlp-protobuf package<br>Example usage:<br>\`\`\`php<br>$transport = otlp_grpc_transport(<br>    endpoint: \'localhost:4317\',<br>    serializer: otlp_protobuf_serializer(),<br>);<br>\`\`\`<br>@param string $endpoint gRPC endpoint (e.g., \'localhost:4317\')<br>@param ProtobufSerializer $serializer Protobuf serializer for encoding telemetry data<br>@param array<string, string> $headers Additional headers (metadata) to include in requests<br>@param bool $insecure Whether to use insecure channel credentials (default true for local dev)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Telemetry\\OTLP\\DSL\\otlp_grpc_transport(" + "$" + "{" + "1:endpoint" + "}" + ", " + "$" + "{" + "2:serializer" + "}" + ", " + "$" + "{" + "3:headers" + "}" + ", " + "$" + "{" + "4:insecure" + "}" + ")"),
        boost: 10
    },                {
        label: "otlp_http_transport",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">otlp_http_transport</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ClientInterface</span> <span class=\"fn-param\">$client</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">RequestFactoryInterface</span> <span class=\"fn-param\">$requestFactory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StreamFactoryInterface</span> <span class=\"fn-param\">$streamFactory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$endpoint</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Serializer</span> <span class=\"fn-param\">$serializer</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$headers</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HttpTransport</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an HTTP transport for OTLP endpoints.<br>Creates an HttpTransport configured to send telemetry data to an OTLP-compatible<br>endpoint using PSR-18 HTTP client. Supports both JSON and Protobuf formats.<br>Example usage:<br>\`\`\`php<br>// JSON over HTTP<br>$transport = otlp_http_transport(<br>    client: $client,<br>    requestFactory: $psr17Factory,<br>    streamFactory: $psr17Factory,<br>    endpoint: \'http://localhost:4318\',<br>    serializer: otlp_json_serializer(),<br>);<br>// Protobuf over HTTP<br>$transport = otlp_http_transport(<br>    client: $client,<br>    requestFactory: $psr17Factory,<br>    streamFactory: $psr17Factory,<br>    endpoint: \'http://localhost:4318\',<br>    serializer: otlp_protobuf_serializer(),<br>);<br>\`\`\`<br>@param ClientInterface $client PSR-18 HTTP client<br>@param RequestFactoryInterface $requestFactory PSR-17 request factory<br>@param StreamFactoryInterface $streamFactory PSR-17 stream factory<br>@param string $endpoint OTLP endpoint URL (e.g., \'http://localhost:4318\')<br>@param Serializer $serializer Serializer for encoding telemetry data (JSON or Protobuf)<br>@param array<string, string> $headers Additional headers to include in requests
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Telemetry\\OTLP\\DSL\\otlp_http_transport(" + "$" + "{" + "1:client" + "}" + ", " + "$" + "{" + "2:requestFactory" + "}" + ", " + "$" + "{" + "3:streamFactory" + "}" + ", " + "$" + "{" + "4:endpoint" + "}" + ", " + "$" + "{" + "5:serializer" + "}" + ", " + "$" + "{" + "6:headers" + "}" + ")"),
        boost: 10
    },                {
        label: "otlp_json_serializer",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">otlp_json_serializer</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonSerializer</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a JSON serializer for OTLP.<br>Returns a JsonSerializer that converts telemetry data to OTLP JSON wire format.<br>Use this with HttpTransport for JSON over HTTP.<br>Example usage:<br>\`\`\`php<br>$serializer = otlp_json_serializer();<br>$transport = otlp_http_transport($client, $reqFactory, $streamFactory, $endpoint, $serializer);<br>\`\`\`
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Telemetry\\OTLP\\DSL\\otlp_json_serializer()"),
        boost: 10
    },                {
        label: "otlp_logger_provider",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">otlp_logger_provider</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">LogProcessor</span> <span class=\"fn-param\">$processor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ClockInterface</span> <span class=\"fn-param\">$clock</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ContextStorage</span> <span class=\"fn-param\">$contextStorage</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Telemetry\\Context\\MemoryContextStorage::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LoggerProvider</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a logger provider configured for OTLP export.<br>Example usage:<br>\`\`\`php<br>$processor = batching_log_processor(otlp_log_exporter($transport));<br>$provider = otlp_logger_provider($processor, $clock);<br>$logger = $provider->logger($resource, \'my-service\', \'1.0.0\');<br>\`\`\`<br>@param LogProcessor $processor The processor for handling log records<br>@param ClockInterface $clock The clock for timestamps<br>@param ContextStorage $contextStorage The context storage for propagating context
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Telemetry\\OTLP\\DSL\\otlp_logger_provider(" + "$" + "{" + "1:processor" + "}" + ", " + "$" + "{" + "2:clock" + "}" + ", " + "$" + "{" + "3:contextStorage" + "}" + ")"),
        boost: 10
    },                {
        label: "otlp_log_exporter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">otlp_log_exporter</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Transport</span> <span class=\"fn-param\">$transport</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OTLPLogExporter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an OTLP log exporter.<br>Example usage:<br>\`\`\`php<br>$exporter = otlp_log_exporter($transport);<br>$processor = batching_log_processor($exporter);<br>\`\`\`<br>@param Transport $transport The transport for sending log data
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Telemetry\\OTLP\\DSL\\otlp_log_exporter(" + "$" + "{" + "1:transport" + "}" + ")"),
        boost: 10
    },                {
        label: "otlp_meter_provider",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">otlp_meter_provider</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">MetricProcessor</span> <span class=\"fn-param\">$processor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ClockInterface</span> <span class=\"fn-param\">$clock</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">AggregationTemporality</span> <span class=\"fn-param\">$temporality</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Telemetry\\Meter\\AggregationTemporality::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MeterProvider</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a meter provider configured for OTLP export.<br>Example usage:<br>\`\`\`php<br>$processor = batching_metric_processor(otlp_metric_exporter($transport));<br>$provider = otlp_meter_provider($processor, $clock);<br>$meter = $provider->meter($resource, \'my-service\', \'1.0.0\');<br>\`\`\`<br>@param MetricProcessor $processor The processor for handling metrics<br>@param ClockInterface $clock The clock for timestamps<br>@param AggregationTemporality $temporality The aggregation temporality for metrics
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Telemetry\\OTLP\\DSL\\otlp_meter_provider(" + "$" + "{" + "1:processor" + "}" + ", " + "$" + "{" + "2:clock" + "}" + ", " + "$" + "{" + "3:temporality" + "}" + ")"),
        boost: 10
    },                {
        label: "otlp_metric_exporter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">otlp_metric_exporter</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Transport</span> <span class=\"fn-param\">$transport</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OTLPMetricExporter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an OTLP metric exporter.<br>Example usage:<br>\`\`\`php<br>$exporter = otlp_metric_exporter($transport);<br>$processor = batching_metric_processor($exporter);<br>\`\`\`<br>@param Transport $transport The transport for sending metric data
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Telemetry\\OTLP\\DSL\\otlp_metric_exporter(" + "$" + "{" + "1:transport" + "}" + ")"),
        boost: 10
    },                {
        label: "otlp_protobuf_serializer",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">otlp_protobuf_serializer</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ProtobufSerializer</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a Protobuf serializer for OTLP.<br>Returns a ProtobufSerializer that converts telemetry data to OTLP Protobuf binary format.<br>Use this with HttpTransport for Protobuf over HTTP, or with GrpcTransport.<br>Requires:<br>- google/protobuf package<br>- open-telemetry/gen-otlp-protobuf package<br>Example usage:<br>\`\`\`php<br>$serializer = otlp_protobuf_serializer();<br>$transport = otlp_http_transport($client, $reqFactory, $streamFactory, $endpoint, $serializer);<br>\`\`\`
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Telemetry\\OTLP\\DSL\\otlp_protobuf_serializer()"),
        boost: 10
    },                {
        label: "otlp_span_exporter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">otlp_span_exporter</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Transport</span> <span class=\"fn-param\">$transport</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OTLPSpanExporter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an OTLP span exporter.<br>Example usage:<br>\`\`\`php<br>$exporter = otlp_span_exporter($transport);<br>$processor = batching_span_processor($exporter);<br>\`\`\`<br>@param Transport $transport The transport for sending span data
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Telemetry\\OTLP\\DSL\\otlp_span_exporter(" + "$" + "{" + "1:transport" + "}" + ")"),
        boost: 10
    },                {
        label: "otlp_tracer_provider",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">otlp_tracer_provider</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SpanProcessor</span> <span class=\"fn-param\">$processor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ClockInterface</span> <span class=\"fn-param\">$clock</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Sampler</span> <span class=\"fn-param\">$sampler</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Telemetry\\Tracer\\Sampler\\AlwaysOnSampler::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ContextStorage</span> <span class=\"fn-param\">$contextStorage</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Telemetry\\Context\\MemoryContextStorage::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TracerProvider</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a tracer provider configured for OTLP export.<br>Example usage:<br>\`\`\`php<br>$processor = batching_span_processor(otlp_span_exporter($transport));<br>$provider = otlp_tracer_provider($processor, $clock);<br>$tracer = $provider->tracer($resource, \'my-service\', \'1.0.0\');<br>\`\`\`<br>@param SpanProcessor $processor The processor for handling spans<br>@param ClockInterface $clock The clock for timestamps<br>@param Sampler $sampler The sampler for deciding whether to record spans<br>@param ContextStorage $contextStorage The context storage for propagating trace context
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Telemetry\\OTLP\\DSL\\otlp_tracer_provider(" + "$" + "{" + "1:processor" + "}" + ", " + "$" + "{" + "2:clock" + "}" + ", " + "$" + "{" + "3:sampler" + "}" + ", " + "$" + "{" + "4:contextStorage" + "}" + ")"),
        boost: 10
    },                {
        label: "overwrite",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">overwrite</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for save_mode_overwrite().
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\overwrite()"),
        boost: 10
    },                {
        label: "pagination_key_asc",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pagination_key_asc</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ParameterType|Type|string|int</span> <span class=\"fn-param\">$type</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Doctrine\\DBAL\\ParameterType::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Key</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\pagination_key_asc(" + "$" + "{" + "1:column" + "}" + ", " + "$" + "{" + "2:type" + "}" + ")"),
        boost: 10
    },                {
        label: "pagination_key_desc",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pagination_key_desc</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ParameterType|Type|string|int</span> <span class=\"fn-param\">$type</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Doctrine\\DBAL\\ParameterType::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Key</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\pagination_key_desc(" + "$" + "{" + "1:column" + "}" + ", " + "$" + "{" + "2:type" + "}" + ")"),
        boost: 10
    },                {
        label: "pagination_key_set",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pagination_key_set</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Key</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">KeySet</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\pagination_key_set(" + "$" + "{" + "1:keys" + "}" + ")"),
        boost: 10
    },                {
        label: "param",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">param</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$position</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Parameter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a positional parameter ($1, $2, etc.).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\param(" + "$" + "{" + "1:position" + "}" + ")"),
        boost: 10
    },                {
        label: "partition",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">partition</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Partition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\partition(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ")"),
        boost: 10
    },                {
        label: "partitions",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">partitions</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Partition</span> <span class=\"fn-param\">$partition</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Partitions</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\partitions(" + "$" + "{" + "1:partition" + "}" + ")"),
        boost: 10
    },                {
        label: "pass_through_log_processor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pass_through_log_processor</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">LogExporter</span> <span class=\"fn-param\">$exporter</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PassThroughLogProcessor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a PassThroughLogProcessor.<br>Exports each log record immediately when processed.<br>Useful for debugging where immediate visibility is more important than performance.<br>@param LogExporter $exporter The exporter to send logs to
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\pass_through_log_processor(" + "$" + "{" + "1:exporter" + "}" + ")"),
        boost: 10
    },                {
        label: "pass_through_metric_processor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pass_through_metric_processor</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">MetricExporter</span> <span class=\"fn-param\">$exporter</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PassThroughMetricProcessor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a PassThroughMetricProcessor.<br>Exports each metric immediately when processed.<br>Useful for debugging where immediate visibility is more important than performance.<br>@param MetricExporter $exporter The exporter to send metrics to
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\pass_through_metric_processor(" + "$" + "{" + "1:exporter" + "}" + ")"),
        boost: 10
    },                {
        label: "pass_through_span_processor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pass_through_span_processor</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SpanExporter</span> <span class=\"fn-param\">$exporter</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PassThroughSpanProcessor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a PassThroughSpanProcessor.<br>Exports each span immediately when it ends.<br>Useful for debugging where immediate visibility is more important than performance.<br>@param SpanExporter $exporter The exporter to send spans to
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\pass_through_span_processor(" + "$" + "{" + "1:exporter" + "}" + ")"),
        boost: 10
    },                {
        label: "path",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">path</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options|array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Path</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Path supports glob patterns.<br>Examples:<br> - path(\'*.csv\') - any csv file in current directory<br> - path(\'/** / *.csv\') - any csv file in any subdirectory (remove empty spaces)<br> - path(\'/dir/partition=* /*.parquet\') - any parquet file in given partition directory.<br>Glob pattern is also supported by remote filesystems like Azure<br> - path(\'azure-blob://directory/*.csv\') - any csv file in given directory<br>@param array<string, null|bool|float|int|string|\\UnitEnum>|Path\\Options $options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\path(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:options" + "}" + ")"),
        boost: 10
    },                {
        label: "path_memory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">path_memory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$path</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Path</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a path to php memory stream.<br>@param string $path - default = \'\' - path is used as an identifier in memory filesystem, so we can write multiple files to memory at once, each path is a new handle<br>@param null|array{\'stream\': \'memory\'|\'temp\'} $options - when nothing is provided, \'temp\' stream is used by default<br>@return Path
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\path_memory(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:options" + "}" + ")"),
        boost: 10
    },                {
        label: "path_real",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">path_real</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Path</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Resolve real path from given path.<br>@param array<string, null|bool|float|int|string|\\UnitEnum> $options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\path_real(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:options" + "}" + ")"),
        boost: 10
    },                {
        label: "path_stdout",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">path_stdout</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Path</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a path to php stdout stream.<br>@param null|array{\'stream\': \'output\'|\'stderr\'|\'stdout\'} $options<br>@return Path
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\path_stdout(" + "$" + "{" + "1:options" + "}" + ")"),
        boost: 10
    },                {
        label: "pgsql_client",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_client</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ConnectionParameters</span> <span class=\"fn-param\">$params</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ValueConverters</span> <span class=\"fn-param\">$valueConverters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">RowMapper</span> <span class=\"fn-param\">$mapper</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Client</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a PostgreSQL client using ext-pgsql.<br>The client connects immediately and is ready to execute queries.<br>For object mapping, provide a RowMapper (use pgsql_mapper() for the default).<br>@param Client\\ConnectionParameters $params Connection parameters<br>@param null|ValueConverters $valueConverters Custom type converters (optional)<br>@param null|Client\\RowMapper $mapper Row mapper for object hydration (optional)<br>@throws ConnectionException If connection fails<br>@example<br>// Basic client<br>$client = pgsql_client(pgsql_connection(\'host=localhost dbname=mydb\'));<br>// With object mapping<br>$client = pgsql_client(<br>    pgsql_connection(\'host=localhost dbname=mydb\'),<br>    mapper: pgsql_mapper(),<br>);
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_client(" + "$" + "{" + "1:params" + "}" + ", " + "$" + "{" + "2:valueConverters" + "}" + ", " + "$" + "{" + "3:mapper" + "}" + ")"),
        boost: 10
    },                {
        label: "pgsql_connection",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_connection</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$connectionString</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConnectionParameters</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create connection parameters from a connection string.<br>Accepts libpq-style connection strings:<br>- Key-value format: \"host=localhost port=5432 dbname=mydb user=myuser password=secret\"<br>- URI format: \"postgresql://user:password@localhost:5432/dbname\"<br>@example<br>$params = pgsql_connection(\'host=localhost dbname=mydb\');<br>$params = pgsql_connection(\'postgresql://user:pass@localhost/mydb\');
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_connection(" + "$" + "{" + "1:connectionString" + "}" + ")"),
        boost: 10
    },                {
        label: "pgsql_connection_dsn",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_connection_dsn</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$dsn</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConnectionParameters</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create connection parameters from a DSN string.<br>Parses standard PostgreSQL DSN format commonly used in environment variables<br>(e.g., DATABASE_URL). Supports postgres://, postgresql://, and pgsql:// schemes.<br>@param string $dsn DSN string in format: postgres://user:password@host:port/database?options<br>@throws Client\\DsnParserException If the DSN cannot be parsed<br>@example<br>$params = pgsql_connection_dsn(\'postgres://myuser:secret@localhost:5432/mydb\');<br>$params = pgsql_connection_dsn(\'postgresql://user:pass@db.example.com/app?sslmode=require\');<br>$params = pgsql_connection_dsn(\'pgsql://user:pass@localhost/mydb\'); // Symfony/Doctrine format<br>$params = pgsql_connection_dsn(getenv(\'DATABASE_URL\'));
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_connection_dsn(" + "$" + "{" + "1:dsn" + "}" + ")"),
        boost: 10
    },                {
        label: "pgsql_connection_params",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_connection_params</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$database</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$host</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;localhost&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$port</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">5432</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$user</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$password</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConnectionParameters</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create connection parameters from individual values.<br>Allows specifying connection parameters individually for better type safety<br>and IDE support.<br>@param string $database Database name (required)<br>@param string $host Hostname (default: localhost)<br>@param int $port Port number (default: 5432)<br>@param null|string $user Username (optional)<br>@param null|string $password Password (optional)<br>@param array<string, string> $options Additional libpq options<br>@example<br>$params = pgsql_connection_params(<br>    database: \'mydb\',<br>    host: \'localhost\',<br>    user: \'myuser\',<br>    password: \'secret\',<br>);
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_connection_params(" + "$" + "{" + "1:database" + "}" + ", " + "$" + "{" + "2:host" + "}" + ", " + "$" + "{" + "3:port" + "}" + ", " + "$" + "{" + "4:user" + "}" + ", " + "$" + "{" + "5:password" + "}" + ", " + "$" + "{" + "6:options" + "}" + ")"),
        boost: 10
    },                {
        label: "pgsql_mapper",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_mapper</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConstructorMapper</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a default constructor-based row mapper.<br>Maps database rows directly to constructor parameters.<br>Column names must match parameter names exactly (1:1).<br>Use SQL aliases if column names differ from parameter names.<br>@example<br>// DTO where column names match parameter names<br>readonly class User {<br>    public function __construct(<br>        public int $id,<br>        public string $name,<br>        public string $email,<br>    ) {}<br>}<br>// Usage<br>$client = pgsql_client(pgsql_connection(\'...\'), mapper: pgsql_mapper());<br>// For snake_case columns, use SQL aliases<br>$user = $client->fetchInto(<br>    User::class,<br>    \'SELECT id, user_name AS name, user_email AS email FROM users WHERE id = $1\',<br>    [1]<br>);
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_mapper()"),
        boost: 10
    },                {
        label: "pgsql_type_bigint",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_bigint</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_bigint()"),
        boost: 10
    },                {
        label: "pgsql_type_bit",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_bit</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_bit()"),
        boost: 10
    },                {
        label: "pgsql_type_bool",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_bool</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_bool()"),
        boost: 10
    },                {
        label: "pgsql_type_boolean",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_boolean</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_boolean()"),
        boost: 10
    },                {
        label: "pgsql_type_bool_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_bool_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_bool_array()"),
        boost: 10
    },                {
        label: "pgsql_type_bpchar",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_bpchar</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_bpchar()"),
        boost: 10
    },                {
        label: "pgsql_type_bytea",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_bytea</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_bytea()"),
        boost: 10
    },                {
        label: "pgsql_type_char",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_char</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_char()"),
        boost: 10
    },                {
        label: "pgsql_type_cidr",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_cidr</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_cidr()"),
        boost: 10
    },                {
        label: "pgsql_type_date",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_date</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_date()"),
        boost: 10
    },                {
        label: "pgsql_type_double",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_double</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_double()"),
        boost: 10
    },                {
        label: "pgsql_type_float4",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_float4</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_float4()"),
        boost: 10
    },                {
        label: "pgsql_type_float4_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_float4_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_float4_array()"),
        boost: 10
    },                {
        label: "pgsql_type_float8",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_float8</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_float8()"),
        boost: 10
    },                {
        label: "pgsql_type_float8_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_float8_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_float8_array()"),
        boost: 10
    },                {
        label: "pgsql_type_inet",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_inet</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_inet()"),
        boost: 10
    },                {
        label: "pgsql_type_int2",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_int2</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_int2()"),
        boost: 10
    },                {
        label: "pgsql_type_int2_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_int2_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_int2_array()"),
        boost: 10
    },                {
        label: "pgsql_type_int4",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_int4</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_int4()"),
        boost: 10
    },                {
        label: "pgsql_type_int4_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_int4_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_int4_array()"),
        boost: 10
    },                {
        label: "pgsql_type_int8",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_int8</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_int8()"),
        boost: 10
    },                {
        label: "pgsql_type_int8_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_int8_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_int8_array()"),
        boost: 10
    },                {
        label: "pgsql_type_integer",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_integer</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_integer()"),
        boost: 10
    },                {
        label: "pgsql_type_interval",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_interval</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_interval()"),
        boost: 10
    },                {
        label: "pgsql_type_json",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_json</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_json()"),
        boost: 10
    },                {
        label: "pgsql_type_jsonb",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_jsonb</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_jsonb()"),
        boost: 10
    },                {
        label: "pgsql_type_jsonb_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_jsonb_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_jsonb_array()"),
        boost: 10
    },                {
        label: "pgsql_type_json_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_json_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_json_array()"),
        boost: 10
    },                {
        label: "pgsql_type_macaddr",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_macaddr</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_macaddr()"),
        boost: 10
    },                {
        label: "pgsql_type_macaddr8",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_macaddr8</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_macaddr8()"),
        boost: 10
    },                {
        label: "pgsql_type_money",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_money</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_money()"),
        boost: 10
    },                {
        label: "pgsql_type_numeric",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_numeric</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_numeric()"),
        boost: 10
    },                {
        label: "pgsql_type_oid",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_oid</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_oid()"),
        boost: 10
    },                {
        label: "pgsql_type_real",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_real</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_real()"),
        boost: 10
    },                {
        label: "pgsql_type_smallint",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_smallint</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_smallint()"),
        boost: 10
    },                {
        label: "pgsql_type_text",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_text</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_text()"),
        boost: 10
    },                {
        label: "pgsql_type_text_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_text_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_text_array()"),
        boost: 10
    },                {
        label: "pgsql_type_time",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_time</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_time()"),
        boost: 10
    },                {
        label: "pgsql_type_timestamp",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_timestamp</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_timestamp()"),
        boost: 10
    },                {
        label: "pgsql_type_timestamptz",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_timestamptz</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_timestamptz()"),
        boost: 10
    },                {
        label: "pgsql_type_timetz",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_timetz</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_timetz()"),
        boost: 10
    },                {
        label: "pgsql_type_uuid",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_uuid</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_uuid()"),
        boost: 10
    },                {
        label: "pgsql_type_uuid_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_uuid_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_uuid_array()"),
        boost: 10
    },                {
        label: "pgsql_type_varbit",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_varbit</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_varbit()"),
        boost: 10
    },                {
        label: "pgsql_type_varchar",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_varchar</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_varchar()"),
        boost: 10
    },                {
        label: "pgsql_type_varchar_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_varchar_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_varchar_array()"),
        boost: 10
    },                {
        label: "pgsql_type_xml",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pgsql_type_xml</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlType</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\pgsql_type_xml()"),
        boost: 10
    },                {
        label: "pie_chart",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pie_chart</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference</span> <span class=\"fn-param\">$label</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">References</span> <span class=\"fn-param\">$datasets</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PieChart</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\ChartJS\\pie_chart(" + "$" + "{" + "1:label" + "}" + ", " + "$" + "{" + "2:datasets" + "}" + ")"),
        boost: 10
    },                {
        label: "postgresql_insert_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">postgresql_insert_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$skip_conflicts</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$constraint</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$conflict_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$update_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSQLInsertOptions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string> $conflict_columns<br>@param array<string> $update_columns
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\postgresql_insert_options(" + "$" + "{" + "1:skip_conflicts" + "}" + ", " + "$" + "{" + "2:constraint" + "}" + ", " + "$" + "{" + "3:conflict_columns" + "}" + ", " + "$" + "{" + "4:update_columns" + "}" + ")"),
        boost: 10
    },                {
        label: "postgresql_telemetry_config",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">postgresql_telemetry_config</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Telemetry</span> <span class=\"fn-param\">$telemetry</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ClockInterface</span> <span class=\"fn-param\">$clock</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">PostgreSqlTelemetryOptions</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlTelemetryConfig</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create telemetry configuration for PostgreSQL client.<br>Bundles telemetry instance, clock, and options needed to instrument a PostgreSQL client.<br>@param Telemetry $telemetry The telemetry instance<br>@param ClockInterface $clock Clock for timestamps<br>@param null|PostgreSqlTelemetryOptions $options Telemetry options (default: all enabled)<br>@example<br>$config = postgresql_telemetry_config(<br>    telemetry(resource([\'service.name\' => \'my-app\'])),<br>    new SystemClock(),<br>);
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\postgresql_telemetry_config(" + "$" + "{" + "1:telemetry" + "}" + ", " + "$" + "{" + "2:clock" + "}" + ", " + "$" + "{" + "3:options" + "}" + ")"),
        boost: 10
    },                {
        label: "postgresql_telemetry_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">postgresql_telemetry_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$traceQueries</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$traceTransactions</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$collectMetrics</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$logQueries</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$maxQueryLength</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1000</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$includeParameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$maxParameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">10</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$maxParameterLength</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">100</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSqlTelemetryOptions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create telemetry options for PostgreSQL client instrumentation.<br>Controls which telemetry signals (traces, metrics, logs) are enabled<br>and how query information is captured.<br>@param bool $traceQueries Create spans for query execution (default: true)<br>@param bool $traceTransactions Create spans for transactions (default: true)<br>@param bool $collectMetrics Collect duration and row count metrics (default: true)<br>@param bool $logQueries Log executed queries (default: false)<br>@param null|int $maxQueryLength Maximum query text length in telemetry (default: 1000, null = unlimited)<br>@param bool $includeParameters Include query parameters in telemetry (default: false, security consideration)<br>@example<br>// Default options (traces and metrics enabled)<br>$options = postgresql_telemetry_options();<br>// Enable query logging<br>$options = postgresql_telemetry_options(logQueries: true);<br>// Disable all but metrics<br>$options = postgresql_telemetry_options(<br>    traceQueries: false,<br>    traceTransactions: false,<br>    collectMetrics: true,<br>);
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\postgresql_telemetry_options(" + "$" + "{" + "1:traceQueries" + "}" + ", " + "$" + "{" + "2:traceTransactions" + "}" + ", " + "$" + "{" + "3:collectMetrics" + "}" + ", " + "$" + "{" + "4:logQueries" + "}" + ", " + "$" + "{" + "5:maxQueryLength" + "}" + ", " + "$" + "{" + "6:includeParameters" + "}" + ", " + "$" + "{" + "7:maxParameters" + "}" + ", " + "$" + "{" + "8:maxParameterLength" + "}" + ")"),
        boost: 10
    },                {
        label: "postgresql_update_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">postgresql_update_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$primary_key_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$update_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PostgreSQLUpdateOptions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string> $primary_key_columns<br>@param array<string> $update_columns
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\postgresql_update_options(" + "$" + "{" + "1:primary_key_columns" + "}" + ", " + "$" + "{" + "2:update_columns" + "}" + ")"),
        boost: 10
    },                {
        label: "prepare_transaction",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">prepare_transaction</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$transactionId</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PreparedTransactionFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a PREPARE TRANSACTION builder.<br>Example: prepare_transaction(\'my_transaction\')<br>Produces: PREPARE TRANSACTION \'my_transaction\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\prepare_transaction(" + "$" + "{" + "1:transactionId" + "}" + ")"),
        boost: 10
    },                {
        label: "primary_key",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">primary_key</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$columns</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PrimaryKeyConstraint</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a PRIMARY KEY constraint.<br>@param string ...$columns Columns that form the primary key
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\primary_key(" + "$" + "{" + "1:columns" + "}" + ")"),
        boost: 10
    },                {
        label: "print_rows",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">print_rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Rows</span> <span class=\"fn-param\">$rows</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\print_rows(" + "$" + "{" + "1:rows" + "}" + ", " + "$" + "{" + "2:truncate" + "}" + ", " + "$" + "{" + "3:formatter" + "}" + ")"),
        boost: 10
    },                {
        label: "print_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">print_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Schema $schema<br>@deprecated Please use schema_to_ascii($schema) instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\print_schema(" + "$" + "{" + "1:schema" + "}" + ", " + "$" + "{" + "2:formatter" + "}" + ")"),
        boost: 10
    },                {
        label: "process_detector",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">process_detector</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ProcessDetector</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a ProcessDetector.<br>Detects process information including process.pid, process.executable.path,<br>process.runtime.name (PHP), process.runtime.version, process.command,<br>and process.owner (on POSIX systems).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\process_detector()"),
        boost: 10
    },                {
        label: "propagation_context",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">propagation_context</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SpanContext</span> <span class=\"fn-param\">$spanContext</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Baggage</span> <span class=\"fn-param\">$baggage</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PropagationContext</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a PropagationContext.<br>Value object containing both trace context (SpanContext) and application<br>data (Baggage) that can be propagated across process boundaries.<br>@param null|SpanContext $spanContext Optional span context<br>@param null|Baggage $baggage Optional baggage
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\propagation_context(" + "$" + "{" + "1:spanContext" + "}" + ", " + "$" + "{" + "2:baggage" + "}" + ")"),
        boost: 10
    },                {
        label: "protocol",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">protocol</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$protocol</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Protocol</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\protocol(" + "$" + "{" + "1:protocol" + "}" + ")"),
        boost: 10
    },                {
        label: "psr7_request_carrier",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">psr7_request_carrier</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ServerRequestInterface</span> <span class=\"fn-param\">$request</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RequestCarrier</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Psr7\\Telemetry\\DSL\\psr7_request_carrier(" + "$" + "{" + "1:request" + "}" + ")"),
        boost: 10
    },                {
        label: "psr7_response_carrier",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">psr7_response_carrier</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ResponseInterface</span> <span class=\"fn-param\">$response</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ResponseCarrier</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Psr7\\Telemetry\\DSL\\psr7_response_carrier(" + "$" + "{" + "1:response" + "}" + ")"),
        boost: 10
    },                {
        label: "psr18_traceable_client",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">psr18_traceable_client</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ClientInterface</span> <span class=\"fn-param\">$client</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Telemetry</span> <span class=\"fn-param\">$telemetry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PSR18TraceableClient</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Psr18\\Telemetry\\DSL\\psr18_traceable_client(" + "$" + "{" + "1:client" + "}" + ", " + "$" + "{" + "2:telemetry" + "}" + ")"),
        boost: 10
    },                {
        label: "random_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">random_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$length</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">RandomValueGenerator</span> <span class=\"fn-param\">$generator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\NativePHPRandomValueGenerator::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RandomString</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\random_string(" + "$" + "{" + "1:length" + "}" + ", " + "$" + "{" + "2:generator" + "}" + ")"),
        boost: 10
    },                {
        label: "rank",
        type: "function",
        detail: "flow\u002Ddsl\u002Dwindow\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rank</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Rank</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\rank()"),
        boost: 10
    },                {
        label: "raw_cond",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">raw_cond</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RawCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a raw SQL condition (use with caution).<br>SECURITY WARNING: This function accepts raw SQL without parameterization.<br>SQL injection is possible if used with untrusted user input.<br>Only use with trusted, validated input.<br>For user-provided values, use standard condition functions with param():<br>\`\`\`php<br>// UNSAFE - SQL injection possible:<br>raw_cond(\"status = \'\" . $userInput . \"\'\")<br>// SAFE - use typed conditions:<br>eq(col(\'status\'), param(1))<br>\`\`\`
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\raw_cond(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },                {
        label: "raw_expr",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">raw_expr</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RawExpression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a raw SQL expression (use with caution).<br>SECURITY WARNING: This function accepts raw SQL without parameterization.<br>SQL injection is possible if used with untrusted user input.<br>Only use with trusted, validated input.<br>For user-provided values, use param() instead:<br>\`\`\`php<br>// UNSAFE - SQL injection possible:<br>raw_expr(\"custom_func(\'\" . $userInput . \"\')\")<br>// SAFE - use parameters:<br>func(\'custom_func\', param(1))<br>\`\`\`
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\raw_expr(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },                {
        label: "reassign_owned",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">reassign_owned</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$roles</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ReassignOwnedToStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a REASSIGN OWNED builder.<br>Example: reassign_owned(\'old_role\')->to(\'new_role\')<br>Produces: REASSIGN OWNED BY old_role TO new_role<br>@param string ...$roles The roles whose owned objects should be reassigned<br>@return ReassignOwnedToStep Builder for reassign owned options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\reassign_owned(" + "$" + "{" + "1:roles" + "}" + ")"),
        boost: 10
    },                {
        label: "ref",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ref</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EntryReference</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\ref(" + "$" + "{" + "1:entry" + "}" + ")"),
        boost: 10
    },                {
        label: "refresh_materialized_view",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">refresh_materialized_view</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RefreshMatViewOptionsStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a REFRESH MATERIALIZED VIEW builder.<br>Example: refresh_materialized_view(\'user_stats\')<br>Produces: REFRESH MATERIALIZED VIEW user_stats<br>Example: refresh_materialized_view(\'user_stats\')->concurrently()->withData()<br>Produces: REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats WITH DATA<br>@param string $name View name (may include schema as \"schema.view\")<br>@param null|string $schema Schema name (optional, overrides parsed schema)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\refresh_materialized_view(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "refs",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">refs</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">References</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\refs(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },                {
        label: "ref_action_cascade",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ref_action_cascade</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ReferentialAction</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Get a CASCADE referential action.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\ref_action_cascade()"),
        boost: 10
    },                {
        label: "ref_action_no_action",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ref_action_no_action</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ReferentialAction</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Get a NO ACTION referential action.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\ref_action_no_action()"),
        boost: 10
    },                {
        label: "ref_action_restrict",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ref_action_restrict</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ReferentialAction</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Get a RESTRICT referential action.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\ref_action_restrict()"),
        boost: 10
    },                {
        label: "ref_action_set_default",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ref_action_set_default</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ReferentialAction</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Get a SET DEFAULT referential action.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\ref_action_set_default()"),
        boost: 10
    },                {
        label: "ref_action_set_null",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ref_action_set_null</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ReferentialAction</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Get a SET NULL referential action.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\ref_action_set_null()"),
        boost: 10
    },                {
        label: "regex",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regex</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Regex</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\regex(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:subject" + "}" + ", " + "$" + "{" + "3:flags" + "}" + ", " + "$" + "{" + "4:offset" + "}" + ")"),
        boost: 10
    },                {
        label: "regex_all",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regex_all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexAll</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\regex_all(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:subject" + "}" + ", " + "$" + "{" + "3:flags" + "}" + ", " + "$" + "{" + "4:offset" + "}" + ")"),
        boost: 10
    },                {
        label: "regex_imatch",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regex_imatch</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OperatorCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a POSIX regex match condition (~*).<br>Case-insensitive.<br>Example: regex_imatch(col(\'email\'), literal_string(\'.*@gmail\\\\.com\'))<br>Produces: email ~* \'.*@gmail\\.com\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\regex_imatch(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:pattern" + "}" + ")"),
        boost: 10
    },                {
        label: "regex_match",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regex_match</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexMatch</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\regex_match(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:subject" + "}" + ", " + "$" + "{" + "3:flags" + "}" + ", " + "$" + "{" + "4:offset" + "}" + ")"),
        boost: 10
    },                {
        label: "regex_match",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regex_match</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OperatorCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a POSIX regex match condition (~).<br>Case-sensitive.<br>Example: regex_match(col(\'email\'), literal_string(\'.*@gmail\\\\.com\'))<br>Produces: email ~ \'.*@gmail\\.com\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\regex_match(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:pattern" + "}" + ")"),
        boost: 10
    },                {
        label: "regex_match_all",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regex_match_all</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexMatchAll</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\regex_match_all(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:subject" + "}" + ", " + "$" + "{" + "3:flags" + "}" + ", " + "$" + "{" + "4:offset" + "}" + ")"),
        boost: 10
    },                {
        label: "regex_replace",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regex_replace</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$replacement</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$subject</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int|null</span> <span class=\"fn-param\">$limit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexReplace</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\regex_replace(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:replacement" + "}" + ", " + "$" + "{" + "3:subject" + "}" + ", " + "$" + "{" + "4:limit" + "}" + ")"),
        boost: 10
    },                {
        label: "reindex_database",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">reindex_database</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ReindexFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Start building a REINDEX DATABASE statement.<br>Use chainable methods: ->concurrently(), ->verbose(), ->tablespace()<br>Example: reindex_database(\'mydb\')->concurrently()<br>@param string $name The database name
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\reindex_database(" + "$" + "{" + "1:name" + "}" + ")"),
        boost: 10
    },                {
        label: "reindex_index",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">reindex_index</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ReindexFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Start building a REINDEX INDEX statement.<br>Use chainable methods: ->concurrently(), ->verbose(), ->tablespace()<br>Example: reindex_index(\'idx_users_email\')->concurrently()<br>@param string $name The index name (may include schema: schema.index)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\reindex_index(" + "$" + "{" + "1:name" + "}" + ")"),
        boost: 10
    },                {
        label: "reindex_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">reindex_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ReindexFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Start building a REINDEX SCHEMA statement.<br>Use chainable methods: ->concurrently(), ->verbose(), ->tablespace()<br>Example: reindex_schema(\'public\')->concurrently()<br>@param string $name The schema name
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\reindex_schema(" + "$" + "{" + "1:name" + "}" + ")"),
        boost: 10
    },                {
        label: "reindex_table",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">reindex_table</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ReindexFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Start building a REINDEX TABLE statement.<br>Use chainable methods: ->concurrently(), ->verbose(), ->tablespace()<br>Example: reindex_table(\'users\')->concurrently()<br>@param string $name The table name (may include schema: schema.table)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\reindex_table(" + "$" + "{" + "1:name" + "}" + ")"),
        boost: 10
    },                {
        label: "release_savepoint",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">release_savepoint</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SavepointFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Release a SAVEPOINT.<br>Example: release_savepoint(\'my_savepoint\')<br>Produces: RELEASE my_savepoint
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\release_savepoint(" + "$" + "{" + "1:name" + "}" + ")"),
        boost: 10
    },                {
        label: "rename_replace",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rename_replace</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array|string</span> <span class=\"fn-param\">$search</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array|string</span> <span class=\"fn-param\">$replace</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RenameReplaceEntryStrategy</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string>|string $search<br>@param array<string>|string $replace
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\rename_replace(" + "$" + "{" + "1:search" + "}" + ", " + "$" + "{" + "2:replace" + "}" + ")"),
        boost: 10
    },                {
        label: "rename_style",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rename_style</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">StringStyles|StringStyles</span> <span class=\"fn-param\">$style</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RenameCaseEntryStrategy</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\rename_style(" + "$" + "{" + "1:style" + "}" + ")"),
        boost: 10
    },                {
        label: "reset_role",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">reset_role</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ResetRoleFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a RESET ROLE builder.<br>Example: reset_role()<br>Produces: RESET ROLE<br>@return ResetRoleFinalStep Builder for reset role
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\reset_role()"),
        boost: 10
    },                {
        label: "resource",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">resource</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Attributes|array</span> <span class=\"fn-param\">$attributes</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Resource</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a Resource.<br>@param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes Resource attributes
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\resource(" + "$" + "{" + "1:attributes" + "}" + ")"),
        boost: 10
    },                {
        label: "resource_detector",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">resource_detector</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$detectors</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChainDetector</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a resource detector chain.<br>When no detectors are provided, uses the default detector chain:<br>1. OsDetector - Operating system information<br>2. HostDetector - Host information<br>3. ProcessDetector - Process information<br>4. ComposerDetector - Service information from Composer<br>5. EnvironmentDetector - Environment variable overrides (highest precedence)<br>When detectors are provided, uses only those detectors.<br>@param array<ResourceDetector> $detectors Optional custom detectors (empty = use defaults)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\resource_detector(" + "$" + "{" + "1:detectors" + "}" + ")"),
        boost: 10
    },                {
        label: "retry_any_throwable",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">retry_any_throwable</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AnyThrowable</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\retry_any_throwable(" + "$" + "{" + "1:limit" + "}" + ")"),
        boost: 10
    },                {
        label: "retry_on_exception_types",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">retry_on_exception_types</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$exception_types</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OnExceptionTypes</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<class-string<\\Throwable>> $exception_types
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\retry_on_exception_types(" + "$" + "{" + "1:exception_types" + "}" + ", " + "$" + "{" + "2:limit" + "}" + ")"),
        boost: 10
    },                {
        label: "returning",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">returning</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expressions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ReturningClause</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a RETURNING clause.<br>@param Expression ...$expressions Expressions to return
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\returning(" + "$" + "{" + "1:expressions" + "}" + ")"),
        boost: 10
    },                {
        label: "returning_all",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">returning_all</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ReturningClause</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a RETURNING * clause.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\returning_all()"),
        boost: 10
    },                {
        label: "revoke",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">revoke</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">TablePrivilege|string</span> <span class=\"fn-param\">$privileges</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RevokeOnStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a REVOKE privileges builder.<br>Example: revoke(TablePrivilege::SELECT)->onTable(\'users\')->from(\'app_user\')<br>Produces: REVOKE SELECT ON users FROM app_user<br>Example: revoke(TablePrivilege::ALL)->onTable(\'users\')->from(\'app_user\')->cascade()<br>Produces: REVOKE ALL ON users FROM app_user CASCADE<br>@param string|TablePrivilege ...$privileges The privileges to revoke<br>@return RevokeOnStep Builder for revoke options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\revoke(" + "$" + "{" + "1:privileges" + "}" + ")"),
        boost: 10
    },                {
        label: "revoke_role",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">revoke_role</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$roles</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RevokeRoleFromStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a REVOKE role builder.<br>Example: revoke_role(\'admin\')->from(\'user1\')<br>Produces: REVOKE admin FROM user1<br>Example: revoke_role(\'admin\')->from(\'user1\')->cascade()<br>Produces: REVOKE admin FROM user1 CASCADE<br>@param string ...$roles The roles to revoke<br>@return RevokeRoleFromStep Builder for revoke role options
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\revoke_role(" + "$" + "{" + "1:roles" + "}" + ")"),
        boost: 10
    },                {
        label: "rollback",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rollback</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RollbackOptionsStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a ROLLBACK transaction builder.<br>Example: rollback()->toSavepoint(\'my_savepoint\')<br>Produces: ROLLBACK TO SAVEPOINT my_savepoint
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\rollback()"),
        boost: 10
    },                {
        label: "rollback_prepared",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rollback_prepared</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$transactionId</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PreparedTransactionFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a ROLLBACK PREPARED builder.<br>Example: rollback_prepared(\'my_transaction\')<br>Produces: ROLLBACK PREPARED \'my_transaction\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\rollback_prepared(" + "$" + "{" + "1:transactionId" + "}" + ")"),
        boost: 10
    },                {
        label: "round",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">round</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int|float</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$precision</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">2</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$mode</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Round</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\round(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:precision" + "}" + ", " + "$" + "{" + "3:mode" + "}" + ")"),
        boost: 10
    },                {
        label: "row",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">row</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Entry</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Row</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Entry<mixed> ...$entry
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\row(" + "$" + "{" + "1:entry" + "}" + ")"),
        boost: 10
    },                {
        label: "rows",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Row</span> <span class=\"fn-param\">$row</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Rows</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\rows(" + "$" + "{" + "1:row" + "}" + ")"),
        boost: 10
    },                {
        label: "rows_partitioned",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rows_partitioned</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$rows</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Partitions|array</span> <span class=\"fn-param\">$partitions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Rows</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<Row> $rows<br>@param array<\\Flow\\Filesystem\\Partition|string>|Partitions $partitions
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\rows_partitioned(" + "$" + "{" + "1:rows" + "}" + ", " + "$" + "{" + "2:partitions" + "}" + ")"),
        boost: 10
    },                {
        label: "row_expr",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">row_expr</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$elements</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RowExpression</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a row expression.<br>@param list<Expression> $elements Row elements
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\row_expr(" + "$" + "{" + "1:elements" + "}" + ")"),
        boost: 10
    },                {
        label: "row_number",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">row_number</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RowNumber</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\row_number()"),
        boost: 10
    },                {
        label: "sanitize",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sanitize</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$placeholder</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;*&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int|null</span> <span class=\"fn-param\">$skipCharacters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Sanitize</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\sanitize(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:placeholder" + "}" + ", " + "$" + "{" + "3:skipCharacters" + "}" + ")"),
        boost: 10
    },                {
        label: "savepoint",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">savepoint</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SavepointFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SAVEPOINT.<br>Example: savepoint(\'my_savepoint\')<br>Produces: SAVEPOINT my_savepoint
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\savepoint(" + "$" + "{" + "1:name" + "}" + ")"),
        boost: 10
    },                {
        label: "save_mode_append",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">save_mode_append</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\save_mode_append()"),
        boost: 10
    },                {
        label: "save_mode_exception_if_exists",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">save_mode_exception_if_exists</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\save_mode_exception_if_exists()"),
        boost: 10
    },                {
        label: "save_mode_ignore",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">save_mode_ignore</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\save_mode_ignore()"),
        boost: 10
    },                {
        label: "save_mode_overwrite",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">save_mode_overwrite</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SaveMode</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\save_mode_overwrite()"),
        boost: 10
    },                {
        label: "schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Definition</span> <span class=\"fn-param\">$definitions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Definition<mixed> ...$definitions<br>@return Schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema(" + "$" + "{" + "1:definitions" + "}" + ")"),
        boost: 10
    },                {
        label: "schema_evolving_validator",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_evolving_validator</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EvolvingValidator</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_evolving_validator()"),
        boost: 10
    },                {
        label: "schema_from_json",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_from_json</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_from_json(" + "$" + "{" + "1:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "schema_from_parquet",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_from_parquet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Parquet\\schema_from_parquet(" + "$" + "{" + "1:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "schema_metadata",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_metadata</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Metadata</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string, array<bool|float|int|string>|bool|float|int|string> $metadata
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_metadata(" + "$" + "{" + "1:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "schema_selective_validator",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_selective_validator</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SelectiveValidator</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_selective_validator()"),
        boost: 10
    },                {
        label: "schema_strict_validator",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_strict_validator</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StrictValidator</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_strict_validator()"),
        boost: 10
    },                {
        label: "schema_to_ascii",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_to_ascii</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Schema $schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_to_ascii(" + "$" + "{" + "1:schema" + "}" + ", " + "$" + "{" + "2:formatter" + "}" + ")"),
        boost: 10
    },                {
        label: "schema_to_json",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_to_json</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$pretty</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Schema $schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_to_json(" + "$" + "{" + "1:schema" + "}" + ", " + "$" + "{" + "2:pretty" + "}" + ")"),
        boost: 10
    },                {
        label: "schema_to_parquet",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_to_parquet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Parquet\\schema_to_parquet(" + "$" + "{" + "1:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "schema_to_php",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_to_php</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ValueFormatter</span> <span class=\"fn-param\">$valueFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Schema\\Formatter\\PHPFormatter\\ValueFormatter::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">TypeFormatter</span> <span class=\"fn-param\">$typeFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Schema\\Formatter\\PHPFormatter\\TypeFormatter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Schema $schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_to_php(" + "$" + "{" + "1:schema" + "}" + ", " + "$" + "{" + "2:valueFormatter" + "}" + ", " + "$" + "{" + "3:typeFormatter" + "}" + ")"),
        boost: 10
    },                {
        label: "schema_validate",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema_validate</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$expected</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$given</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaValidator</span> <span class=\"fn-param\">$validator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Schema\\Validator\\StrictValidator::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Schema $expected<br>@param Schema $given
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\schema_validate(" + "$" + "{" + "1:expected" + "}" + ", " + "$" + "{" + "2:given" + "}" + ", " + "$" + "{" + "3:validator" + "}" + ")"),
        boost: 10
    },                {
        label: "select",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtransformers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">select</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Select</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\select(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },                {
        label: "select",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">select</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expressions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SelectBuilder</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a new SELECT query builder.<br>@param Expression ...$expressions Columns to select. If empty, returns SelectSelectStep.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\select(" + "$" + "{" + "1:expressions" + "}" + ")"),
        boost: 10
    },                {
        label: "set_role",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">set_role</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$role</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SetRoleFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SET ROLE builder.<br>Example: set_role(\'admin\')<br>Produces: SET ROLE admin<br>@param string $role The role to set<br>@return SetRoleFinalStep Builder for set role
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\set_role(" + "$" + "{" + "1:role" + "}" + ")"),
        boost: 10
    },                {
        label: "set_session_transaction",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">set_session_transaction</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SetTransactionOptionsStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SET SESSION CHARACTERISTICS AS TRANSACTION builder.<br>Example: set_session_transaction()->isolationLevel(IsolationLevel::SERIALIZABLE)<br>Produces: SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\set_session_transaction()"),
        boost: 10
    },                {
        label: "set_transaction",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">set_transaction</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SetTransactionOptionsStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SET TRANSACTION builder.<br>Example: set_transaction()->isolationLevel(IsolationLevel::SERIALIZABLE)->readOnly()<br>Produces: SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\set_transaction()"),
        boost: 10
    },                {
        label: "severity_filtering_log_processor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">severity_filtering_log_processor</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">LogProcessor</span> <span class=\"fn-param\">$processor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Severity</span> <span class=\"fn-param\">$minimumSeverity</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Telemetry\\Logger\\Severity::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SeverityFilteringLogProcessor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SeverityFilteringLogProcessor.<br>Filters log entries based on minimum severity level. Only entries at or above<br>the configured threshold are passed to the wrapped processor.<br>@param LogProcessor $processor The processor to wrap<br>@param Severity $minimumSeverity Minimum severity level (default: INFO)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\severity_filtering_log_processor(" + "$" + "{" + "1:processor" + "}" + ", " + "$" + "{" + "2:minimumSeverity" + "}" + ")"),
        boost: 10
    },                {
        label: "severity_mapper",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">severity_mapper</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$customMapping</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SeverityMapper</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SeverityMapper for mapping Monolog levels to Telemetry severities.<br>@param null|array<int, Severity> $customMapping Optional custom mapping (Monolog Level value => Telemetry Severity)<br>Example with default mapping:<br>\`\`\`php<br>$mapper = severity_mapper();<br>\`\`\`<br>Example with custom mapping:<br>\`\`\`php<br>use Monolog\\Level;<br>use Flow\\Telemetry\\Logger\\Severity;<br>$mapper = severity_mapper([<br>    Level::Debug->value => Severity::DEBUG,<br>    Level::Info->value => Severity::INFO,<br>    Level::Notice->value => Severity::WARN,  // Custom: NOTICE → WARN instead of INFO<br>    Level::Warning->value => Severity::WARN,<br>    Level::Error->value => Severity::ERROR,<br>    Level::Critical->value => Severity::FATAL,<br>    Level::Alert->value => Severity::FATAL,<br>    Level::Emergency->value => Severity::FATAL,<br>]);<br>\`\`\`
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Monolog\\Telemetry\\DSL\\severity_mapper(" + "$" + "{" + "1:customMapping" + "}" + ")"),
        boost: 10
    },                {
        label: "similar_to",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">similar_to</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$expr</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SimilarTo</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SIMILAR TO condition.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\similar_to(" + "$" + "{" + "1:expr" + "}" + ", " + "$" + "{" + "2:pattern" + "}" + ")"),
        boost: 10
    },                {
        label: "size",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">size</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Size</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\size(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },                {
        label: "skip_rows_handler",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">skip_rows_handler</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SkipRows</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\skip_rows_handler()"),
        boost: 10
    },                {
        label: "span_context",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">span_context</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">TraceId</span> <span class=\"fn-param\">$traceId</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SpanId</span> <span class=\"fn-param\">$spanId</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SpanId</span> <span class=\"fn-param\">$parentSpanId</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SpanContext</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SpanContext.<br>@param TraceId $traceId The trace ID<br>@param SpanId $spanId The span ID<br>@param null|SpanId $parentSpanId Optional parent span ID
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\span_context(" + "$" + "{" + "1:traceId" + "}" + ", " + "$" + "{" + "2:spanId" + "}" + ", " + "$" + "{" + "3:parentSpanId" + "}" + ")"),
        boost: 10
    },                {
        label: "span_event",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">span_event</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateTimeImmutable</span> <span class=\"fn-param\">$timestamp</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Attributes|array</span> <span class=\"fn-param\">$attributes</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">GenericEvent</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SpanEvent (GenericEvent) with an explicit timestamp.<br>@param string $name Event name<br>@param \\DateTimeImmutable $timestamp Event timestamp<br>@param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes Event attributes
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\span_event(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:timestamp" + "}" + ", " + "$" + "{" + "3:attributes" + "}" + ")"),
        boost: 10
    },                {
        label: "span_id",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">span_id</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$hex</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SpanId</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SpanId.<br>If a hex string is provided, creates a SpanId from it.<br>Otherwise, generates a new random SpanId.<br>@param null|string $hex Optional 16-character hexadecimal string<br>@throws \\InvalidArgumentException if the hex string is invalid
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\span_id(" + "$" + "{" + "1:hex" + "}" + ")"),
        boost: 10
    },                {
        label: "span_limits",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">span_limits</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$attributeCountLimit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">128</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$eventCountLimit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">128</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$linkCountLimit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">128</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$attributePerEventCountLimit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">128</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$attributePerLinkCountLimit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">128</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$attributeValueLengthLimit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SpanLimits</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create SpanLimits configuration.<br>SpanLimits controls the maximum amount of data a span can collect,<br>preventing unbounded memory growth and ensuring reasonable span sizes.<br>@param int $attributeCountLimit Maximum number of attributes per span<br>@param int $eventCountLimit Maximum number of events per span<br>@param int $linkCountLimit Maximum number of links per span<br>@param int $attributePerEventCountLimit Maximum number of attributes per event<br>@param int $attributePerLinkCountLimit Maximum number of attributes per link<br>@param null|int $attributeValueLengthLimit Maximum length for string attribute values (null = unlimited)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\span_limits(" + "$" + "{" + "1:attributeCountLimit" + "}" + ", " + "$" + "{" + "2:eventCountLimit" + "}" + ", " + "$" + "{" + "3:linkCountLimit" + "}" + ", " + "$" + "{" + "4:attributePerEventCountLimit" + "}" + ", " + "$" + "{" + "5:attributePerLinkCountLimit" + "}" + ", " + "$" + "{" + "6:attributeValueLengthLimit" + "}" + ")"),
        boost: 10
    },                {
        label: "span_link",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">span_link</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SpanContext</span> <span class=\"fn-param\">$context</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Attributes|array</span> <span class=\"fn-param\">$attributes</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SpanLink</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SpanLink.<br>@param SpanContext $context The linked span context<br>@param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes Link attributes
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\span_link(" + "$" + "{" + "1:context" + "}" + ", " + "$" + "{" + "2:attributes" + "}" + ")"),
        boost: 10
    },                {
        label: "split",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">split</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$separator</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$limit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">9223372036854775807</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Split</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\split(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:separator" + "}" + ", " + "$" + "{" + "3:limit" + "}" + ")"),
        boost: 10
    },                {
        label: "sprintf",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sprintf</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$format</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string|int|float|null</span> <span class=\"fn-param\">$args</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Sprintf</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\sprintf(" + "$" + "{" + "1:format" + "}" + ", " + "$" + "{" + "2:args" + "}" + ")"),
        boost: 10
    },                {
        label: "sqlite_insert_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sqlite_insert_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$skip_conflicts</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$conflict_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$update_columns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SqliteInsertOptions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string> $conflict_columns<br>@param array<string> $update_columns
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\sqlite_insert_options(" + "$" + "{" + "1:skip_conflicts" + "}" + ", " + "$" + "{" + "2:conflict_columns" + "}" + ", " + "$" + "{" + "3:update_columns" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_analyze",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_analyze</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Plan</span> <span class=\"fn-param\">$plan</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">PlanAnalyzer</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a plan analyzer for analyzing EXPLAIN plans.<br>@param Plan $plan The execution plan to analyze<br>@return PlanAnalyzer The analyzer for extracting insights
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_analyze(" + "$" + "{" + "1:plan" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_deparse",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_deparse</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ParsedQuery</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DeparseOptions</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Convert a ParsedQuery AST back to SQL string.<br>When called without options, returns the SQL as a simple string.<br>When called with DeparseOptions, applies formatting (pretty-printing, indentation, etc.).<br>@throws \\RuntimeException if deparsing fails
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_deparse(" + "$" + "{" + "1:query" + "}" + ", " + "$" + "{" + "2:options" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_deparse_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_deparse_options</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DeparseOptions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create DeparseOptions for configuring SQL formatting.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_deparse_options()"),
        boost: 10
    },                {
        label: "sql_explain_config",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_explain_config</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$analyze</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$verbose</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$costs</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$buffers</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$timing</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ExplainFormat</span> <span class=\"fn-param\">$format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\PostgreSql\\QueryBuilder\\Utility\\ExplainFormat::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ExplainConfig</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an ExplainConfig for customizing EXPLAIN options.<br>@param bool $analyze Whether to actually execute the query (ANALYZE)<br>@param bool $verbose Include verbose output<br>@param bool $costs Include cost estimates (default true)<br>@param bool $buffers Include buffer usage statistics (requires analyze)<br>@param bool $timing Include timing information (requires analyze)<br>@param ExplainFormat $format Output format (JSON recommended for parsing)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_explain_config(" + "$" + "{" + "1:analyze" + "}" + ", " + "$" + "{" + "2:verbose" + "}" + ", " + "$" + "{" + "3:costs" + "}" + ", " + "$" + "{" + "4:buffers" + "}" + ", " + "$" + "{" + "5:timing" + "}" + ", " + "$" + "{" + "6:format" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_explain_modifier",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_explain_modifier</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ExplainConfig</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ExplainModifier</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create an ExplainModifier for transforming queries into EXPLAIN queries.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_explain_modifier(" + "$" + "{" + "1:config" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_explain_parse",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_explain_parse</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$jsonOutput</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Plan</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Parse EXPLAIN JSON output into a Plan object.<br>@param string $jsonOutput The JSON output from EXPLAIN (FORMAT JSON)<br>@return Plan The parsed execution plan
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_explain_parse(" + "$" + "{" + "1:jsonOutput" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_fingerprint",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_fingerprint</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Returns a fingerprint of the given SQL query.<br>Literal values are normalized so they won\'t affect the fingerprint.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_fingerprint(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_format",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_format</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DeparseOptions</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Parse and format SQL query with pretty printing.<br>This is a convenience function that parses SQL and returns it formatted.<br>@param string $sql The SQL query to format<br>@param null|DeparseOptions $options Formatting options (defaults to pretty-print enabled)<br>@throws \\RuntimeException if parsing or deparsing fails
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_format(" + "$" + "{" + "1:sql" + "}" + ", " + "$" + "{" + "2:options" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_keyset_column",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_keyset_column</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SortOrder</span> <span class=\"fn-param\">$order</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\PostgreSql\\AST\\Transformers\\SortOrder::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">KeysetColumn</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a KeysetColumn for keyset pagination.<br>@param string $column Column name (can include table alias like \"u.id\")<br>@param SortOrder $order Sort order (ASC or DESC)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_keyset_column(" + "$" + "{" + "1:column" + "}" + ", " + "$" + "{" + "2:order" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_normalize",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_normalize</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Normalize SQL query by replacing literal values and named parameters with positional parameters.<br>WHERE id = :id will be changed into WHERE id = $1<br>WHERE id = 1 will be changed into WHERE id = $1.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_normalize(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_normalize_utility",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_normalize_utility</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Normalize utility SQL statements (DDL like CREATE, ALTER, DROP).<br>This handles DDL statements differently from pg_normalize() which is optimized for DML.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_normalize_utility(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_parse",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_parse</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ParsedQuery</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_parse(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_parser",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_parser</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Parser</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_parser()"),
        boost: 10
    },                {
        label: "sql_query_columns",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_query_columns</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ParsedQuery</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Columns</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Extract columns from a parsed SQL query.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_query_columns(" + "$" + "{" + "1:query" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_query_depth",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_query_depth</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Get the maximum nesting depth of a SQL query.<br>Example:<br>- \"SELECT * FROM t\" => 1<br>- \"SELECT * FROM (SELECT * FROM t)\" => 2<br>- \"SELECT * FROM (SELECT * FROM (SELECT * FROM t))\" => 3
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_query_depth(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_query_functions",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_query_functions</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ParsedQuery</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Functions</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Extract functions from a parsed SQL query.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_query_functions(" + "$" + "{" + "1:query" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_query_order_by",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_query_order_by</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ParsedQuery</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OrderBy</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Extract ORDER BY clauses from a parsed SQL query.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_query_order_by(" + "$" + "{" + "1:query" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_query_tables",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_query_tables</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ParsedQuery</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Tables</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Extract tables from a parsed SQL query.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_query_tables(" + "$" + "{" + "1:query" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_split",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_split</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">array</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Split string with multiple SQL statements into array of individual statements.<br>@return array<string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_split(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_summary",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_summary</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$truncateLimit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Generate a summary of parsed queries in protobuf format.<br>Useful for query monitoring and logging without full AST overhead.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_summary(" + "$" + "{" + "1:sql" + "}" + ", " + "$" + "{" + "2:options" + "}" + ", " + "$" + "{" + "3:truncateLimit" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_to_count_query",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_to_count_query</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Transform a SQL query into a COUNT query for pagination.<br>Wraps the query in: SELECT COUNT(*) FROM (...) AS _count_subq<br>Removes ORDER BY and LIMIT/OFFSET from the inner query.<br>@param string $sql The SQL query to transform<br>@return string The COUNT query
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_to_count_query(" + "$" + "{" + "1:sql" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_to_explain",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_to_explain</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ExplainConfig</span> <span class=\"fn-param\">$config</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Transform a SQL query into an EXPLAIN query.<br>Returns the modified SQL with EXPLAIN wrapped around it.<br>Defaults to EXPLAIN ANALYZE with JSON format for easy parsing.<br>@param string $sql The SQL query to explain<br>@param null|ExplainConfig $config EXPLAIN configuration (defaults to forAnalysis())<br>@return string The EXPLAIN query
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_to_explain(" + "$" + "{" + "1:sql" + "}" + ", " + "$" + "{" + "2:config" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_to_keyset_query",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_to_keyset_query</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$columns</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$cursor</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Transform a SQL query into a keyset (cursor-based) paginated query.<br>More efficient than OFFSET for large datasets - uses indexed WHERE conditions.<br>Automatically detects existing query parameters and appends keyset placeholders at the end.<br>@param string $sql The SQL query to paginate (must have ORDER BY)<br>@param int $limit Maximum number of rows to return<br>@param list<KeysetColumn> $columns Columns for keyset pagination (must match ORDER BY)<br>@param null|list<null|bool|float|int|string> $cursor Values from last row of previous page (null for first page)<br>@return string The paginated SQL query
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_to_keyset_query(" + "$" + "{" + "1:sql" + "}" + ", " + "$" + "{" + "2:limit" + "}" + ", " + "$" + "{" + "3:columns" + "}" + ", " + "$" + "{" + "4:cursor" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_to_limited_query",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_to_limited_query</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Transform a SQL query to limit results to a specific number of rows.<br>@param string $sql The SQL query to limit<br>@param int $limit Maximum number of rows to return<br>@return string The limited SQL query
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_to_limited_query(" + "$" + "{" + "1:sql" + "}" + ", " + "$" + "{" + "2:limit" + "}" + ")"),
        boost: 10
    },                {
        label: "sql_to_paginated_query",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sql_to_paginated_query</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$sql</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Transform a SQL query into a paginated query with LIMIT and OFFSET.<br>@param string $sql The SQL query to paginate<br>@param int $limit Maximum number of rows to return<br>@param int $offset Number of rows to skip (requires ORDER BY in query)<br>@return string The paginated SQL query
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sql_to_paginated_query(" + "$" + "{" + "1:sql" + "}" + ", " + "$" + "{" + "2:limit" + "}" + ", " + "$" + "{" + "3:offset" + "}" + ")"),
        boost: 10
    },                {
        label: "star",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">star</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$table</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Star</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SELECT * expression.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\star(" + "$" + "{" + "1:table" + "}" + ")"),
        boost: 10
    },                {
        label: "stdout_filesystem",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stdout_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StdOutFilesystem</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Write-only filesystem useful when we just want to write the output to stdout.<br>The main use case is for streaming datasets over http.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\stdout_filesystem()"),
        boost: 10
    },                {
        label: "string_agg",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">string_agg</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;, &#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SortOrder</span> <span class=\"fn-param\">$sort</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringAggregate</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\string_agg(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:separator" + "}" + ", " + "$" + "{" + "3:sort" + "}" + ")"),
        boost: 10
    },                {
        label: "string_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">string_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\string_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "string_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">string_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\string_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "structure_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">structure_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StructureType</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param ?array<string, mixed> $value<br>@param StructureType<T> $type<br>@return Entry<?array<string, T>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\structure_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:type" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "structure_ref",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">structure_ref</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StructureFunctions</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\structure_ref(" + "$" + "{" + "1:entry" + "}" + ")"),
        boost: 10
    },                {
        label: "structure_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">structure_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StructureType|Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StructureDefinition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param StructureType<T>|Type<array<string, T>> $type<br>@return StructureDefinition<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\structure_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:type" + "}" + ", " + "$" + "{" + "3:nullable" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "struct_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">struct_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StructureType</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param ?array<string, mixed> $value<br>@param StructureType<T> $type<br>@return Entry<?array<string, T>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\struct_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:type" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "struct_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">struct_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">StructureType|Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StructureDefinition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param StructureType<T>|Type<array<string, T>> $type<br>@return StructureDefinition<T><br>@deprecated Use \`structure_schema()\` instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\struct_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:type" + "}" + ", " + "$" + "{" + "3:nullable" + "}" + ", " + "$" + "{" + "4:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "str_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">str_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\str_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "str_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">str_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringDefinition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for \`string_schema\`.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\str_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "sub_select",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sub_select</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SelectFinalStep</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Subquery</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a subquery expression.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\sub_select(" + "$" + "{" + "1:query" + "}" + ")"),
        boost: 10
    },                {
        label: "sum",
        type: "function",
        detail: "flow\u002Ddsl\u002Daggregating\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sum</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">EntryReference|string</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Sum</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\sum(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },                {
        label: "superglobal_carrier",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">superglobal_carrier</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SuperglobalCarrier</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SuperglobalCarrier.<br>Read-only carrier that extracts context from PHP superglobals<br>($_SERVER, $_GET, $_POST, $_COOKIE).
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\superglobal_carrier()"),
        boost: 10
    },                {
        label: "symfony_request_carrier",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">symfony_request_carrier</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Request</span> <span class=\"fn-param\">$request</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RequestCarrier</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Symfony\\HttpFoundationTelemetry\\DSL\\symfony_request_carrier(" + "$" + "{" + "1:request" + "}" + ")"),
        boost: 10
    },                {
        label: "symfony_response_carrier",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">symfony_response_carrier</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Response</span> <span class=\"fn-param\">$response</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ResponseCarrier</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Symfony\\HttpFoundationTelemetry\\DSL\\symfony_response_carrier(" + "$" + "{" + "1:response" + "}" + ")"),
        boost: 10
    },                {
        label: "table",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">table</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Table</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a table reference.<br>Supports dot notation for schema-qualified names: \"public.users\" or explicit schema parameter.<br>Double-quoted identifiers preserve dots: \'\"my.table\"\' creates a single identifier.<br>@param string $name Table name (may include schema as \"schema.table\")<br>@param null|string $schema Schema name (optional, overrides parsed schema)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\table(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "table_func",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">table_func</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">FunctionCall</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$withOrdinality</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TableFunction</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a table function reference.<br>@param FunctionCall $function The table-valued function<br>@param bool $withOrdinality Whether to add WITH ORDINALITY
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\table_func(" + "$" + "{" + "1:function" + "}" + ", " + "$" + "{" + "2:withOrdinality" + "}" + ")"),
        boost: 10
    },                {
        label: "table_schema_to_flow_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">table_schema_to_flow_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Table</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types_map</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Converts a Doctrine\\DBAL\\Schema\\Table to a Flow\\ETL\\Schema.<br>@param array<class-string<\\Flow\\Types\\Type<mixed>>, class-string<\\Doctrine\\DBAL\\Types\\Type>> $types_map<br>@return Schema
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\table_schema_to_flow_schema(" + "$" + "{" + "1:table" + "}" + ", " + "$" + "{" + "2:types_map" + "}" + ")"),
        boost: 10
    },                {
        label: "telemetry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">telemetry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Resource</span> <span class=\"fn-param\">$resource</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">TracerProvider</span> <span class=\"fn-param\">$tracerProvider</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">MeterProvider</span> <span class=\"fn-param\">$meterProvider</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">LoggerProvider</span> <span class=\"fn-param\">$loggerProvider</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Telemetry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a new Telemetry instance with the given providers.<br>If providers are not specified, void providers (no-op) are used.<br>@param resource $resource The resource describing the entity producing telemetry<br>@param null|TracerProvider $tracerProvider The tracer provider (null for void/disabled)<br>@param null|MeterProvider $meterProvider The meter provider (null for void/disabled)<br>@param null|LoggerProvider $loggerProvider The logger provider (null for void/disabled)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\telemetry(" + "$" + "{" + "1:resource" + "}" + ", " + "$" + "{" + "2:tracerProvider" + "}" + ", " + "$" + "{" + "3:meterProvider" + "}" + ", " + "$" + "{" + "4:loggerProvider" + "}" + ")"),
        boost: 10
    },                {
        label: "telemetry_handler",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">telemetry_handler</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Logger</span> <span class=\"fn-param\">$logger</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">LogRecordConverter</span> <span class=\"fn-param\">$converter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Bridge\\Monolog\\Telemetry\\LogRecordConverter::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Level</span> <span class=\"fn-param\">$level</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Monolog\\Level::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$bubble</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TelemetryHandler</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a TelemetryHandler for forwarding Monolog logs to Flow Telemetry.<br>@param Logger $logger The Flow Telemetry logger to forward logs to<br>@param LogRecordConverter $converter Converter to transform Monolog LogRecord to Telemetry LogRecord<br>@param Level $level The minimum logging level at which this handler will be triggered<br>@param bool $bubble Whether messages handled by this handler should bubble up to other handlers<br>Example usage:<br>\`\`\`php<br>use Monolog\\Logger as MonologLogger;<br>use function Flow\\Bridge\\Monolog\\Telemetry\\DSL\\telemetry_handler;<br>use function Flow\\Telemetry\\DSL\\telemetry;<br>$telemetry = telemetry();<br>$logger = $telemetry->logger(\'my-app\');<br>$monolog = new MonologLogger(\'channel\');<br>$monolog->pushHandler(telemetry_handler($logger));<br>$monolog->info(\'User logged in\', [\'user_id\' => 123]);<br>// → Forwarded to Flow Telemetry with INFO severity<br>\`\`\`<br>Example with custom converter:<br>\`\`\`php<br>$converter = log_record_converter(<br>    severityMapper: severity_mapper([<br>        Level::Debug->value => Severity::TRACE,<br>    ])<br>);<br>$monolog->pushHandler(telemetry_handler($logger, $converter));<br>\`\`\`
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Monolog\\Telemetry\\DSL\\telemetry_handler(" + "$" + "{" + "1:logger" + "}" + ", " + "$" + "{" + "2:converter" + "}" + ", " + "$" + "{" + "3:level" + "}" + ", " + "$" + "{" + "4:bubble" + "}" + ")"),
        boost: 10
    },                {
        label: "telemetry_options",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">telemetry_options</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$trace_loading</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$trace_transformations</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$trace_cache</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$collect_metrics</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">FilesystemTelemetryOptions</span> <span class=\"fn-param\">$filesystem</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TelemetryOptions</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\telemetry_options(" + "$" + "{" + "1:trace_loading" + "}" + ", " + "$" + "{" + "2:trace_transformations" + "}" + ", " + "$" + "{" + "3:trace_cache" + "}" + ", " + "$" + "{" + "4:collect_metrics" + "}" + ", " + "$" + "{" + "5:filesystem" + "}" + ")"),
        boost: 10
    },                {
        label: "text_search_match",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">text_search_match</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$document</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$query</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OperatorCondition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a full-text search match condition (@@).<br>Example: text_search_match(col(\'document\'), raw_expr(\"to_tsquery(\'english\', \'hello & world\')\"))<br>Produces: document @@ to_tsquery(\'english\', \'hello & world\')
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\text_search_match(" + "$" + "{" + "1:document" + "}" + ", " + "$" + "{" + "2:query" + "}" + ")"),
        boost: 10
    },                {
        label: "throw_error_handler",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">throw_error_handler</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ThrowError</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\throw_error_handler()"),
        boost: 10
    },                {
        label: "time_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">time_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DateInterval|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\DateInterval>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\time_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "time_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">time_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TimeDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\time_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "to_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_array</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$array</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Convert rows to an array and store them in passed array variable.<br>@param array<array-key, mixed> $array<br>@param-out array<array<mixed>> $array
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_array(" + "$" + "{" + "1:array" + "}" + ")"),
        boost: 10
    },                {
        label: "to_avro",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_avro</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">AvroLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\Adapter\\Avro\\to_avro(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "to_branch",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_branch</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$condition</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loader</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BranchingLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_branch(" + "$" + "{" + "1:condition" + "}" + ", " + "$" + "{" + "2:loader" + "}" + ")"),
        boost: 10
    },                {
        label: "to_callable",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_callable</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">callable</span> <span class=\"fn-param\">$callable</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CallbackLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_callable(" + "$" + "{" + "1:callable" + "}" + ")"),
        boost: 10
    },                {
        label: "to_chartjs",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_chartjs</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Chart</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChartJSLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\ChartJS\\to_chartjs(" + "$" + "{" + "1:type" + "}" + ")"),
        boost: 10
    },                {
        label: "to_chartjs_file",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_chartjs_file</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Chart</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Path|string|null</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Path|string|null</span> <span class=\"fn-param\">$template</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChartJSLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Chart $type<br>@param null|Path|string $output - @deprecated use $loader->withOutputPath() instead<br>@param null|Path|string $template - @deprecated use $loader->withTemplate() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\ChartJS\\to_chartjs_file(" + "$" + "{" + "1:type" + "}" + ", " + "$" + "{" + "2:output" + "}" + ", " + "$" + "{" + "3:template" + "}" + ")"),
        boost: 10
    },                {
        label: "to_chartjs_var",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_chartjs_var</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Chart</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$output</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ChartJSLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Chart $type<br>@param array<array-key, mixed> $output - @deprecated use $loader->withOutputVar() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\ChartJS\\to_chartjs_var(" + "$" + "{" + "1:type" + "}" + ", " + "$" + "{" + "2:output" + "}" + ")"),
        boost: 10
    },                {
        label: "to_csv",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_csv</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$uri</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$with_header</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;,&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$enclosure</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;\\&quot;&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$escape</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;\\\\&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$new_line_separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;\\n&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$datetime_format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d\\\\TH:i:sP&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CSVLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $uri<br>@param bool $with_header - @deprecated use $loader->withHeader() instead<br>@param string $separator - @deprecated use $loader->withSeparator() instead<br>@param string $enclosure - @deprecated use $loader->withEnclosure() instead<br>@param string $escape - @deprecated use $loader->withEscape() instead<br>@param string $new_line_separator - @deprecated use $loader->withNewLineSeparator() instead<br>@param string $datetime_format - @deprecated use $loader->withDateTimeFormat() instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\CSV\\to_csv(" + "$" + "{" + "1:uri" + "}" + ", " + "$" + "{" + "2:with_header" + "}" + ", " + "$" + "{" + "3:separator" + "}" + ", " + "$" + "{" + "4:enclosure" + "}" + ", " + "$" + "{" + "5:escape" + "}" + ", " + "$" + "{" + "6:new_line_separator" + "}" + ", " + "$" + "{" + "7:datetime_format" + "}" + ")"),
        boost: 10
    },                {
        label: "to_date",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_date</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|DateTimeZone</span> <span class=\"fn-param\">$timeZone</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">DateTimeZone::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToDate</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_date(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:format" + "}" + ", " + "$" + "{" + "3:timeZone" + "}" + ")"),
        boost: 10
    },                {
        label: "to_date_time",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_date_time</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d H:i:s&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|DateTimeZone</span> <span class=\"fn-param\">$timeZone</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">DateTimeZone::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToDateTime</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_date_time(" + "$" + "{" + "1:ref" + "}" + ", " + "$" + "{" + "2:format" + "}" + ", " + "$" + "{" + "3:timeZone" + "}" + ")"),
        boost: 10
    },                {
        label: "to_dbal_schema_table",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_dbal_schema_table</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$table_name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$table_options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$types_map</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Table</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Converts a Flow\\ETL\\Schema to a Doctrine\\DBAL\\Schema\\Table.<br>@param Schema $schema<br>@param array<array-key, mixed> $table_options<br>@param array<class-string<\\Flow\\Types\\Type<mixed>>, class-string<\\Doctrine\\DBAL\\Types\\Type>> $types_map
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_schema_table(" + "$" + "{" + "1:schema" + "}" + ", " + "$" + "{" + "2:table_name" + "}" + ", " + "$" + "{" + "3:table_options" + "}" + ", " + "$" + "{" + "4:types_map" + "}" + ")"),
        boost: 10
    },                {
        label: "to_dbal_table_delete",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_dbal_table_delete</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Delete rows from database table based on the provided data.<br>In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array<string, mixed>|Connection $connection<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_table_delete(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:table" + "}" + ")"),
        boost: 10
    },                {
        label: "to_dbal_table_insert",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_dbal_table_insert</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">InsertOptions</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Insert new rows into a database table.<br>Insert can also be used as an upsert with the help of InsertOptions.<br>InsertOptions are platform specific, so please choose the right one for your database.<br> - MySQLInsertOptions<br> - PostgreSQLInsertOptions<br> - SqliteInsertOptions<br>In order to control the size of the single insert, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array<string, mixed>|Connection $connection<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_table_insert(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:table" + "}" + ", " + "$" + "{" + "3:options" + "}" + ")"),
        boost: 10
    },                {
        label: "to_dbal_table_update",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_dbal_table_update</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$table</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">UpdateOptions</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DbalLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                     Update existing rows in database.<br> In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array<string, mixed>|Connection $connection<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_table_update(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:table" + "}" + ", " + "$" + "{" + "3:options" + "}" + ")"),
        boost: 10
    },                {
        label: "to_dbal_transaction",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_dbal_transaction</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Connection|array</span> <span class=\"fn-param\">$connection</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loaders</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TransactionalDbalLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Execute multiple loaders within a database transaction.<br>Each batch of rows will be processed in its own transaction.<br>If any loader fails, the entire batch will be rolled back.<br>@param array<string, mixed>|Connection $connection<br>@param Loader ...$loaders - Loaders to execute within the transaction<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Doctrine\\to_dbal_transaction(" + "$" + "{" + "1:connection" + "}" + ", " + "$" + "{" + "2:loaders" + "}" + ")"),
        boost: 10
    },                {
        label: "to_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">EntryFactory</span> <span class=\"fn-param\">$entryFactory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<mixed> $data<br>@return Entry<mixed>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:data" + "}" + ", " + "$" + "{" + "3:entryFactory" + "}" + ")"),
        boost: 10
    },                {
        label: "to_es_bulk_index",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_es_bulk_index</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$index</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">IdFactory</span> <span class=\"fn-param\">$id_factory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ElasticsearchLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html.<br>In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array{<br> hosts?: array<string>,<br> connectionParams?: array<mixed>,<br> retries?: int,<br> sniffOnStart?: bool,<br> sslCert?: array<string>,<br> sslKey?: array<string>,<br> sslVerification?: bool|string,<br> elasticMetaHeader?: bool,<br> includePortInHostHeader?: bool<br>} $config<br>@param string $index<br>@param IdFactory $id_factory<br>@param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html - @deprecated use withParameters method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Elasticsearch\\to_es_bulk_index(" + "$" + "{" + "1:config" + "}" + ", " + "$" + "{" + "2:index" + "}" + ", " + "$" + "{" + "3:id_factory" + "}" + ", " + "$" + "{" + "4:parameters" + "}" + ")"),
        boost: 10
    },                {
        label: "to_es_bulk_update",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_es_bulk_update</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$config</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$index</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">IdFactory</span> <span class=\"fn-param\">$id_factory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$parameters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ElasticsearchLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                     https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html.<br>In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().<br>@param array{<br> hosts?: array<string>,<br> connectionParams?: array<mixed>,<br> retries?: int,<br> sniffOnStart?: bool,<br> sslCert?: array<string>,<br> sslKey?: array<string>,<br> sslVerification?: bool|string,<br> elasticMetaHeader?: bool,<br> includePortInHostHeader?: bool<br>} $config<br>@param string $index<br>@param IdFactory $id_factory<br>@param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html - @deprecated use withParameters method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Elasticsearch\\to_es_bulk_update(" + "$" + "{" + "1:config" + "}" + ", " + "$" + "{" + "2:index" + "}" + ", " + "$" + "{" + "3:id_factory" + "}" + ", " + "$" + "{" + "4:parameters" + "}" + ")"),
        boost: 10
    },                {
        label: "to_excel",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_excel</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ExcelLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Excel\\DSL\\to_excel(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },                {
        label: "to_json",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_json</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">4194304</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$date_time_format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d\\\\TH:i:sP&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$put_rows_in_new_lines</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path<br>@param int $flags - PHP JSON Flags - @deprecate use withFlags method instead<br>@param string $date_time_format - format for DateTimeInterface::format() - @deprecate use withDateTimeFormat method instead<br>@param bool $put_rows_in_new_lines - if you want to put each row in a new line - @deprecate use withRowsInNewLines method instead<br>@return JsonLoader
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\JSON\\to_json(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:flags" + "}" + ", " + "$" + "{" + "3:date_time_format" + "}" + ", " + "$" + "{" + "4:put_rows_in_new_lines" + "}" + ")"),
        boost: 10
    },                {
        label: "to_json_lines",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_json_lines</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonLinesLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Used to write to a JSON lines https://jsonlines.org/ formatted file.<br>@param Path|string $path<br>@return JsonLinesLoader
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\JSON\\to_json_lines(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },                {
        label: "to_memory",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_memory</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Memory</span> <span class=\"fn-param\">$memory</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MemoryLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_memory(" + "$" + "{" + "1:memory" + "}" + ")"),
        boost: 10
    },                {
        label: "to_output",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_output</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">20</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Output</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Loader\\StreamLoader\\Output::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Formatter\\AsciiTableFormatter::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$schemaFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Row\\Formatter\\ASCIISchemaFormatter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StreamLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_output(" + "$" + "{" + "1:truncate" + "}" + ", " + "$" + "{" + "2:output" + "}" + ", " + "$" + "{" + "3:formatter" + "}" + ", " + "$" + "{" + "4:schemaFormatter" + "}" + ")"),
        boost: 10
    },                {
        label: "to_parquet",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_parquet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Options</span> <span class=\"fn-param\">$options</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Compressions</span> <span class=\"fn-param\">$compressions</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Parquet\\ParquetFile\\Compressions::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ParquetLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path<br>@param null|Options $options - @deprecated use \`withOptions\` method instead<br>@param Compressions $compressions - @deprecated use \`withCompressions\` method instead<br>@param null|Schema $schema - @deprecated use \`withSchema\` method instead
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Parquet\\to_parquet(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:options" + "}" + ", " + "$" + "{" + "3:compressions" + "}" + ", " + "$" + "{" + "4:schema" + "}" + ")"),
        boost: 10
    },                {
        label: "to_stderr",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_stderr</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">20</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Output</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Loader\\StreamLoader\\Output::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Formatter\\AsciiTableFormatter::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$schemaFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Row\\Formatter\\ASCIISchemaFormatter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StreamLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_stderr(" + "$" + "{" + "1:truncate" + "}" + ", " + "$" + "{" + "2:output" + "}" + ", " + "$" + "{" + "3:formatter" + "}" + ", " + "$" + "{" + "4:schemaFormatter" + "}" + ")"),
        boost: 10
    },                {
        label: "to_stdout",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_stdout</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">20</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Output</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Loader\\StreamLoader\\Output::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Formatter\\AsciiTableFormatter::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$schemaFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Row\\Formatter\\ASCIISchemaFormatter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StreamLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_stdout(" + "$" + "{" + "1:truncate" + "}" + ", " + "$" + "{" + "2:output" + "}" + ", " + "$" + "{" + "3:formatter" + "}" + ", " + "$" + "{" + "4:schemaFormatter" + "}" + ")"),
        boost: 10
    },                {
        label: "to_stream",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_stream</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$uri</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">20</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Output</span> <span class=\"fn-param\">$output</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Loader\\StreamLoader\\Output::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$mode</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;w&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Formatter\\AsciiTableFormatter::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$schemaFormatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Row\\Formatter\\ASCIISchemaFormatter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StreamLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_stream(" + "$" + "{" + "1:uri" + "}" + ", " + "$" + "{" + "2:truncate" + "}" + ", " + "$" + "{" + "3:output" + "}" + ", " + "$" + "{" + "4:mode" + "}" + ", " + "$" + "{" + "5:formatter" + "}" + ", " + "$" + "{" + "6:schemaFormatter" + "}" + ")"),
        boost: 10
    },                {
        label: "to_text",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_text</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$new_line_separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;\\n&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Loader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path<br>@param string $new_line_separator - default PHP_EOL - @deprecated use withNewLineSeparator method instead<br>@return Loader
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\Text\\to_text(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:new_line_separator" + "}" + ")"),
        boost: 10
    },                {
        label: "to_timezone",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_timezone</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|DateTimeInterface</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|DateTimeZone|string</span> <span class=\"fn-param\">$timeZone</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToTimeZone</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_timezone(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:timeZone" + "}" + ")"),
        boost: 10
    },                {
        label: "to_transformation",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_transformation</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Transformer|Transformation</span> <span class=\"fn-param\">$transformer</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loader</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TransformerLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\to_transformation(" + "$" + "{" + "1:transformer" + "}" + ", " + "$" + "{" + "2:loader" + "}" + ")"),
        boost: 10
    },                {
        label: "to_xml",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">to_xml</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Path|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$root_element_name</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;rows&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$row_element_name</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;row&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$attribute_prefix</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;_&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$date_time_format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d\\\\TH:i:s.uP&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">XMLWriter</span> <span class=\"fn-param\">$xml_writer</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Adapter\\XML\\XMLWriter\\DOMDocumentWriter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">XMLLoader</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Path|string $path<br>@param string $root_element_name - @deprecated use \`withRootElementName()\` method instead<br>@param string $row_element_name - @deprecated use \`withRowElementName()\` method instead<br>@param string $attribute_prefix - @deprecated use \`withAttributePrefix()\` method instead<br>@param string $date_time_format - @deprecated use \`withDateTimeFormat()\` method instead<br>@param XMLWriter $xml_writer
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\Adapter\\XML\\to_xml(" + "$" + "{" + "1:path" + "}" + ", " + "$" + "{" + "2:root_element_name" + "}" + ", " + "$" + "{" + "3:row_element_name" + "}" + ", " + "$" + "{" + "4:attribute_prefix" + "}" + ", " + "$" + "{" + "5:date_time_format" + "}" + ", " + "$" + "{" + "6:xml_writer" + "}" + ")"),
        boost: 10
    },                {
        label: "traceable_filesystem",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">traceable_filesystem</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Filesystem</span> <span class=\"fn-param\">$filesystem</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">FilesystemTelemetryConfig</span> <span class=\"fn-param\">$telemetryConfig</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TraceableFilesystem</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Wrap a filesystem with telemetry tracing support.<br>All filesystem and stream operations will be traced according to the configuration.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Filesystem\\DSL\\traceable_filesystem(" + "$" + "{" + "1:filesystem" + "}" + ", " + "$" + "{" + "2:telemetryConfig" + "}" + ")"),
        boost: 10
    },                {
        label: "traceable_postgresql_client",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">traceable_postgresql_client</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Client</span> <span class=\"fn-param\">$client</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">PostgreSqlTelemetryConfig</span> <span class=\"fn-param\">$telemetryConfig</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TraceableClient</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Wrap a PostgreSQL client with telemetry instrumentation.<br>Returns a decorator that adds spans, metrics, and logs to all<br>query and transaction operations following OpenTelemetry conventions.<br>@param Client\\Client $client The PostgreSQL client to instrument<br>@param PostgreSqlTelemetryConfig $telemetryConfig Telemetry configuration<br>@example<br>$client = pgsql_client(pgsql_connection(\'host=localhost dbname=mydb\'));<br>$traceableClient = traceable_postgresql_client(<br>    $client,<br>    postgresql_telemetry_config(<br>        telemetry(resource([\'service.name\' => \'my-app\'])),<br>        new SystemClock(),<br>        postgresql_telemetry_options(<br>            traceQueries: true,<br>            traceTransactions: true,<br>            collectMetrics: true,<br>            logQueries: true,<br>            maxQueryLength: 500,<br>        ),<br>    ),<br>);<br>// All operations now traced<br>$traceableClient->transaction(function (Client $client) {<br>    $user = $client->fetchOne(\'SELECT * FROM users WHERE id = $1\', [123]);<br>    $client->execute(\'UPDATE users SET last_login = NOW() WHERE id = $1\', [123]);<br>});
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\traceable_postgresql_client(" + "$" + "{" + "1:client" + "}" + ", " + "$" + "{" + "2:telemetryConfig" + "}" + ")"),
        boost: 10
    },                {
        label: "tracer_provider",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">tracer_provider</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SpanProcessor</span> <span class=\"fn-param\">$processor</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ClockInterface</span> <span class=\"fn-param\">$clock</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ContextStorage</span> <span class=\"fn-param\">$contextStorage</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Sampler</span> <span class=\"fn-param\">$sampler</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Telemetry\\Tracer\\Sampler\\AlwaysOnSampler::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SpanLimits</span> <span class=\"fn-param\">$limits</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\Telemetry\\Tracer\\SpanLimits::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TracerProvider</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a TracerProvider.<br>Creates a provider that uses a SpanProcessor for processing spans.<br>For void/disabled tracing, pass void_processor().<br>For memory-based testing, pass memory_processor() with exporters.<br>@param SpanProcessor $processor The processor for spans<br>@param ClockInterface $clock The clock for timestamps<br>@param ContextStorage $contextStorage Storage for context propagation<br>@param Sampler $sampler Sampling strategy for spans<br>@param SpanLimits $limits Limits for span attributes, events, and links
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\tracer_provider(" + "$" + "{" + "1:processor" + "}" + ", " + "$" + "{" + "2:clock" + "}" + ", " + "$" + "{" + "3:contextStorage" + "}" + ", " + "$" + "{" + "4:sampler" + "}" + ", " + "$" + "{" + "5:limits" + "}" + ")"),
        boost: 10
    },                {
        label: "trace_based_exemplar_filter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">trace_based_exemplar_filter</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TraceBasedExemplarFilter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a TraceBasedExemplarFilter.<br>Records exemplars only when the span is sampled (has SAMPLED trace flag).<br>This is the default filter, balancing exemplar collection with performance.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\trace_based_exemplar_filter()"),
        boost: 10
    },                {
        label: "trace_id",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">trace_id</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$hex</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TraceId</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a TraceId.<br>If a hex string is provided, creates a TraceId from it.<br>Otherwise, generates a new random TraceId.<br>@param null|string $hex Optional 32-character hexadecimal string<br>@throws \\InvalidArgumentException if the hex string is invalid
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\trace_id(" + "$" + "{" + "1:hex" + "}" + ")"),
        boost: 10
    },                {
        label: "transaction_snapshot",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">transaction_snapshot</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$snapshotId</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">SetTransactionFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a SET TRANSACTION SNAPSHOT builder.<br>Example: transaction_snapshot(\'00000003-0000001A-1\')<br>Produces: SET TRANSACTION SNAPSHOT \'00000003-0000001A-1\'
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\transaction_snapshot(" + "$" + "{" + "1:snapshotId" + "}" + ")"),
        boost: 10
    },                {
        label: "truncate_table",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">truncate_table</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$tables</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TruncateFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a TRUNCATE TABLE builder.<br>@param string ...$tables Table names to truncate
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\truncate_table(" + "$" + "{" + "1:tables" + "}" + ")"),
        boost: 10
    },                {
        label: "typed",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">typed</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">PostgreSqlType</span> <span class=\"fn-param\">$targetType</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TypedValue</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Wrap a value with explicit PostgreSQL type information for parameter binding.<br>Use when auto-detection isn\'t sufficient or when you need to specify<br>the exact PostgreSQL type (since one PHP type can map to multiple PostgreSQL types):<br>- int could be INT2, INT4, or INT8<br>- string could be TEXT, VARCHAR, or CHAR<br>- array must always use typed() since auto-detection cannot determine element type<br>- DateTimeInterface could be TIMESTAMP or TIMESTAMPTZ<br>- Json could be JSON or JSONB<br>@param mixed $value The value to bind<br>@param PostgreSqlType $targetType The PostgreSQL type to convert the value to<br>@example<br>$client->fetch(<br>    \'SELECT * FROM users WHERE id = $1 AND tags = $2\',<br>    [<br>        typed(\'550e8400-e29b-41d4-a716-446655440000\', PostgreSqlType::UUID),<br>        typed([\'tag1\', \'tag2\'], PostgreSqlType::TEXT_ARRAY),<br>    ]<br>);
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\typed(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:targetType" + "}" + ")"),
        boost: 10
    },                {
        label: "types",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">types</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$types</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Types</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> ...$types<br>@return Types<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\types(" + "$" + "{" + "1:types" + "}" + ")"),
        boost: 10
    },                {
        label: "type_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_array</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<array<mixed>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_array()"),
        boost: 10
    },                {
        label: "type_attr",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_attr</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DataType</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">TypeAttribute</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Creates a type attribute for composite types.<br>Example: type_attr(\'name\', data_type_text())<br>Produces: name text<br>Example: type_attr(\'description\', data_type_text())->collate(\'en_US\')<br>Produces: description text COLLATE \"en_US\"<br>@param string $name The attribute name<br>@param DataType $type The attribute type<br>@return TypeAttribute Type attribute value object
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\type_attr(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:type" + "}" + ")"),
        boost: 10
    },                {
        label: "type_boolean",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_boolean</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<bool>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_boolean()"),
        boost: 10
    },                {
        label: "type_callable",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_callable</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<callable>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_callable()"),
        boost: 10
    },                {
        label: "type_class_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_class_string</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$class</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T of object<br>@param null|class-string<T> $class<br>@return ($class is null ? Type<class-string> : Type<class-string<T>>)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_class_string(" + "$" + "{" + "1:class" + "}" + ")"),
        boost: 10
    },                {
        label: "type_date",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_date</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @deprecated please use \\Flow\\Types\\DSL\\type_date() : DateType<br>@return Type<\\DateTimeInterface>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\type_date()"),
        boost: 10
    },                {
        label: "type_date",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_date</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<\\DateTimeInterface>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_date()"),
        boost: 10
    },                {
        label: "type_datetime",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_datetime</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<\\DateTimeInterface>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_datetime()"),
        boost: 10
    },                {
        label: "type_enum",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_enum</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$class</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T of UnitEnum<br>@param class-string<T> $class<br>@return Type<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_enum(" + "$" + "{" + "1:class" + "}" + ")"),
        boost: 10
    },                {
        label: "type_equals",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_equals</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$left</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$right</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Type<mixed> $left<br>@param Type<mixed> $right
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_equals(" + "$" + "{" + "1:left" + "}" + ", " + "$" + "{" + "2:right" + "}" + ")"),
        boost: 10
    },                {
        label: "type_float",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_float</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<float>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_float()"),
        boost: 10
    },                {
        label: "type_from_array",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_from_array</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$data</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string, mixed> $data<br>@return Type<mixed>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_from_array(" + "$" + "{" + "1:data" + "}" + ")"),
        boost: 10
    },                {
        label: "type_html",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_html</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<HTMLDocument>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_html()"),
        boost: 10
    },                {
        label: "type_html_element",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_html_element</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<HTMLElement>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_html_element()"),
        boost: 10
    },                {
        label: "type_instance_of",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_instance_of</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$class</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T of object<br>@param class-string<T> $class<br>@return Type<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_instance_of(" + "$" + "{" + "1:class" + "}" + ")"),
        boost: 10
    },                {
        label: "type_int",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_int</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @deprecated please use \\Flow\\Types\\DSL\\type_integer() : IntegerType<br>@return Type<int>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\type_int()"),
        boost: 10
    },                {
        label: "type_integer",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_integer</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<int>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_integer()"),
        boost: 10
    },                {
        label: "type_intersection",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_intersection</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$first</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$second</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$types</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $first<br>@param Type<T> $second<br>@param Type<T> ...$types<br>@return Type<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_intersection(" + "$" + "{" + "1:first" + "}" + ", " + "$" + "{" + "2:second" + "}" + ", " + "$" + "{" + "3:types" + "}" + ")"),
        boost: 10
    },                {
        label: "type_is",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_is</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$typeClass</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $type<br>@param class-string<Type<mixed>> $typeClass
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_is(" + "$" + "{" + "1:type" + "}" + ", " + "$" + "{" + "2:typeClass" + "}" + ")"),
        boost: 10
    },                {
        label: "type_is_any",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_is_any</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$typeClass</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$typeClasses</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $type<br>@param class-string<Type<mixed>> $typeClass<br>@param class-string<Type<mixed>> ...$typeClasses
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_is_any(" + "$" + "{" + "1:type" + "}" + ", " + "$" + "{" + "2:typeClass" + "}" + ", " + "$" + "{" + "3:typeClasses" + "}" + ")"),
        boost: 10
    },                {
        label: "type_is_nullable",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_is_nullable</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">bool</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $type
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_is_nullable(" + "$" + "{" + "1:type" + "}" + ")"),
        boost: 10
    },                {
        label: "type_json",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_json</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<Json>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_json()"),
        boost: 10
    },                {
        label: "type_list",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_list</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$element</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ListType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $element<br>@return ListType<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_list(" + "$" + "{" + "1:element" + "}" + ")"),
        boost: 10
    },                {
        label: "type_literal",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_literal</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string|int|float|bool</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LiteralType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T of bool|float|int|string<br>@param T $value<br>@return LiteralType<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_literal(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },                {
        label: "type_map",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_map</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$key_type</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$value_type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">MapType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template TKey of array-key<br>@template TValue<br>@param Type<TKey> $key_type<br>@param Type<TValue> $value_type<br>@return MapType<TKey, TValue>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_map(" + "$" + "{" + "1:key_type" + "}" + ", " + "$" + "{" + "2:value_type" + "}" + ")"),
        boost: 10
    },                {
        label: "type_mixed",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_mixed</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<mixed>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_mixed()"),
        boost: 10
    },                {
        label: "type_non_empty_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_non_empty_string</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<non-empty-string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_non_empty_string()"),
        boost: 10
    },                {
        label: "type_null",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_null</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<null>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_null()"),
        boost: 10
    },                {
        label: "type_numeric_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_numeric_string</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<numeric-string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_numeric_string()"),
        boost: 10
    },                {
        label: "type_object",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_object</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<object>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_object()"),
        boost: 10
    },                {
        label: "type_optional",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_optional</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param Type<T> $type<br>@return Type<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_optional(" + "$" + "{" + "1:type" + "}" + ")"),
        boost: 10
    },                {
        label: "type_positive_integer",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_positive_integer</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<int<0, max>>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_positive_integer()"),
        boost: 10
    },                {
        label: "type_resource",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_resource</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<resource>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_resource()"),
        boost: 10
    },                {
        label: "type_scalar",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_scalar</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<bool|float|int|string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_scalar()"),
        boost: 10
    },                {
        label: "type_string",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_string</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<string>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_string()"),
        boost: 10
    },                {
        label: "type_structure",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_structure</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$elements</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$optional_elements</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$allow_extra</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StructureType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@param array<string, Type<T>> $elements<br>@param array<string, Type<T>> $optional_elements<br>@return StructureType<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_structure(" + "$" + "{" + "1:elements" + "}" + ", " + "$" + "{" + "2:optional_elements" + "}" + ", " + "$" + "{" + "3:allow_extra" + "}" + ")"),
        boost: 10
    },                {
        label: "type_time",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_time</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<\\DateInterval>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_time()"),
        boost: 10
    },                {
        label: "type_time_zone",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_time_zone</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<\\DateTimeZone>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_time_zone()"),
        boost: 10
    },                {
        label: "type_union",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_union</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$first</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$second</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$types</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @template T<br>@template T<br>@template T<br>@param Type<T> $first<br>@param Type<T> $second<br>@param Type<T> ...$types<br>@return Type<T>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_union(" + "$" + "{" + "1:first" + "}" + ", " + "$" + "{" + "2:second" + "}" + ", " + "$" + "{" + "3:types" + "}" + ")"),
        boost: 10
    },                {
        label: "type_uuid",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_uuid</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<Uuid>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_uuid()"),
        boost: 10
    },                {
        label: "type_xml",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_xml</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<\\DOMDocument>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_xml()"),
        boost: 10
    },                {
        label: "type_xml_element",
        type: "function",
        detail: "flow\u002Ddsl\u002Dtype",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">type_xml_element</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Type</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Type<\\DOMElement>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Types\\DSL\\type_xml_element()"),
        boost: 10
    },                {
        label: "ulid",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ulid</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string|null</span> <span class=\"fn-param\">$value</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Ulid</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\ulid(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },                {
        label: "unique_constraint",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">unique_constraint</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$columns</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">UniqueConstraint</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a UNIQUE constraint.<br>@param string ...$columns Columns that must be unique together
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\unique_constraint(" + "$" + "{" + "1:columns" + "}" + ")"),
        boost: 10
    },                {
        label: "update",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">update</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">UpdateTableStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a new UPDATE query builder.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\update()"),
        boost: 10
    },                {
        label: "upper",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">upper</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToUpper</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\upper(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },                {
        label: "uuid_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">uuid_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Uuid|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\Flow\\Types\\Value\\Uuid>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\uuid_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "uuid_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">uuid_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">UuidDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\uuid_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "uuid_v4",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">uuid_v4</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Uuid</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\uuid_v4()"),
        boost: 10
    },                {
        label: "uuid_v7",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">uuid_v7</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|DateTimeInterface|null</span> <span class=\"fn-param\">$value</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Uuid</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\uuid_v7(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },                {
        label: "vacuum",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">vacuum</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">VacuumFinalStep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a VACUUM builder.<br>Example: vacuum()->table(\'users\')<br>Produces: VACUUM users
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\vacuum()"),
        boost: 10
    },                {
        label: "values_table",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">values_table</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">RowExpression</span> <span class=\"fn-param\">$rows</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ValuesTable</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a VALUES clause as a table reference.<br>Usage:<br>  select()->from(<br>      values_table(<br>          row_expr([literal(1), literal(\'Alice\')]),<br>          row_expr([literal(2), literal(\'Bob\')])<br>      )->as(\'t\', [\'id\', \'name\'])<br>  )<br>Generates: SELECT * FROM (VALUES (1, \'Alice\'), (2, \'Bob\')) AS t(id, name)
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\values_table(" + "$" + "{" + "1:rows" + "}" + ")"),
        boost: 10
    },                {
        label: "value_normalizer",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">value_normalizer</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ValueNormalizer</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a ValueNormalizer for converting arbitrary PHP values to Telemetry attribute types.<br>The normalizer handles:<br>- null → \'null\' string<br>- scalars (string, int, float, bool) → unchanged<br>- DateTimeInterface → unchanged<br>- Throwable → unchanged<br>- arrays → recursively normalized<br>- objects with __toString() → string cast<br>- objects without __toString() → class name<br>- other types → get_debug_type() result<br>Example usage:<br>\`\`\`php<br>$normalizer = value_normalizer();<br>$normalized = $normalizer->normalize($value);<br>\`\`\`
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Bridge\\Monolog\\Telemetry\\DSL\\value_normalizer()"),
        boost: 10
    },                {
        label: "void_log_exporter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">void_log_exporter</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">VoidLogExporter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a VoidLogExporter.<br>No-op log exporter that discards all data.<br>Use this when telemetry export is disabled to minimize overhead.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\void_log_exporter()"),
        boost: 10
    },                {
        label: "void_log_processor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">void_log_processor</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">VoidLogProcessor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a VoidLogProcessor.<br>No-op log processor that discards all data.<br>Use this when logging is disabled to minimize overhead.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\void_log_processor()"),
        boost: 10
    },                {
        label: "void_metric_exporter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">void_metric_exporter</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">VoidMetricExporter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a VoidMetricExporter.<br>No-op metric exporter that discards all data.<br>Use this when telemetry export is disabled to minimize overhead.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\void_metric_exporter()"),
        boost: 10
    },                {
        label: "void_metric_processor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">void_metric_processor</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">VoidMetricProcessor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a VoidMetricProcessor.<br>No-op metric processor that discards all data.<br>Use this when metrics collection is disabled to minimize overhead.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\void_metric_processor()"),
        boost: 10
    },                {
        label: "void_span_exporter",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">void_span_exporter</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">VoidSpanExporter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a VoidSpanExporter.<br>No-op span exporter that discards all data.<br>Use this when telemetry export is disabled to minimize overhead.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\void_span_exporter()"),
        boost: 10
    },                {
        label: "void_span_processor",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">void_span_processor</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">VoidSpanProcessor</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a VoidSpanProcessor.<br>No-op span processor that discards all data.<br>Use this when tracing is disabled to minimize overhead.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\void_span_processor()"),
        boost: 10
    },                {
        label: "w3c_baggage",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">w3c_baggage</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">W3CBaggage</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a W3CBaggage propagator.<br>Implements W3C Baggage specification for propagating application-specific<br>key-value pairs using the baggage header.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\w3c_baggage()"),
        boost: 10
    },                {
        label: "w3c_trace_context",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">w3c_trace_context</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">W3CTraceContext</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a W3CTraceContext propagator.<br>Implements W3C Trace Context specification for propagating trace context<br>using traceparent and tracestate headers.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\Telemetry\\DSL\\w3c_trace_context()"),
        boost: 10
    },                {
        label: "when",
        type: "function",
        detail: "flow\u002Ddsl\u002Dscalar\u002Dfunctions",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">when</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$condition</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$then</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$else</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">When</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\when(" + "$" + "{" + "1:condition" + "}" + ", " + "$" + "{" + "2:then" + "}" + ", " + "$" + "{" + "3:else" + "}" + ")"),
        boost: 10
    },                {
        label: "when",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">when</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$condition</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$result</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">WhenClause</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a WHEN clause for CASE expression.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\when(" + "$" + "{" + "1:condition" + "}" + ", " + "$" + "{" + "2:result" + "}" + ")"),
        boost: 10
    },                {
        label: "window",
        type: "function",
        detail: "flow\u002Ddsl\u002Ddata\u002Dframe",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">window</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Window</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\window()"),
        boost: 10
    },                {
        label: "window_def",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">window_def</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$partitionBy</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$orderBy</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">WindowFrame</span> <span class=\"fn-param\">$frame</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">WindowDefinition</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a window definition for WINDOW clause.<br>@param string $name Window name<br>@param list<Expression> $partitionBy PARTITION BY expressions<br>@param list<OrderBy> $orderBy ORDER BY items<br>@param null|WindowFrame $frame Window frame specification
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\window_def(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:partitionBy" + "}" + ", " + "$" + "{" + "3:orderBy" + "}" + ", " + "$" + "{" + "4:frame" + "}" + ")"),
        boost: 10
    },                {
        label: "window_frame",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">window_frame</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">FrameMode</span> <span class=\"fn-param\">$mode</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">FrameBound</span> <span class=\"fn-param\">$start</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">FrameBound</span> <span class=\"fn-param\">$end</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">FrameExclusion</span> <span class=\"fn-param\">$exclusion</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\PostgreSql\\QueryBuilder\\Clause\\FrameExclusion::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">WindowFrame</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a window frame specification.
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\window_frame(" + "$" + "{" + "1:mode" + "}" + ", " + "$" + "{" + "2:start" + "}" + ", " + "$" + "{" + "3:end" + "}" + ", " + "$" + "{" + "4:exclusion" + "}" + ")"),
        boost: 10
    },                {
        label: "window_func",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">window_func</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$args</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$partitionBy</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$orderBy</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">WindowFunction</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a window function.<br>@param string $name Function name<br>@param list<Expression> $args Function arguments<br>@param list<Expression> $partitionBy PARTITION BY expressions<br>@param list<OrderBy> $orderBy ORDER BY items
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\window_func(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:args" + "}" + ", " + "$" + "{" + "3:partitionBy" + "}" + ", " + "$" + "{" + "4:orderBy" + "}" + ")"),
        boost: 10
    },                {
        label: "with",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">with</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">CTE</span> <span class=\"fn-param\">$ctes</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">WithBuilder</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Create a WITH clause builder for CTEs.<br>Example: with(cte(\'users\', $subquery))->select(star())->from(table(\'users\'))<br>Example: with(cte(\'a\', $q1), cte(\'b\', $q2))->recursive()->select(...)->from(table(\'a\'))
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\PostgreSql\\DSL\\with(" + "$" + "{" + "1:ctes" + "}" + ")"),
        boost: 10
    },                {
        label: "with_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dhelpers",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">with_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">WithEntry</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\with_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:function" + "}" + ")"),
        boost: 10
    },                {
        label: "write_with_retries",
        type: "function",
        detail: "flow\u002Ddsl\u002Dloaders",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">write_with_retries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loader</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">RetryStrategy</span> <span class=\"fn-param\">$retry_strategy</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Retry\\RetryStrategy\\AnyThrowable::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DelayFactory</span> <span class=\"fn-param\">$delay_factory</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Retry\\DelayFactory\\Fixed\\FixedMilliseconds::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Sleep</span> <span class=\"fn-param\">$sleep</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Time\\SystemSleep::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RetryLoader</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\write_with_retries(" + "$" + "{" + "1:loader" + "}" + ", " + "$" + "{" + "2:retry_strategy" + "}" + ", " + "$" + "{" + "3:delay_factory" + "}" + ", " + "$" + "{" + "4:sleep" + "}" + ")"),
        boost: 10
    },                {
        label: "xml_element_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">xml_element_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DOMElement|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\DOMElement>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\xml_element_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "xml_element_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">xml_element_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">XMLElementDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\xml_element_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "xml_entry",
        type: "function",
        detail: "flow\u002Ddsl\u002Dentries",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">xml_entry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">DOMDocument|string|null</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Entry</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Entry<?\\DOMDocument>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\xml_entry(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:value" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    },                {
        label: "xml_schema",
        type: "function",
        detail: "flow\u002Ddsl\u002Dschema",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">xml_schema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$name</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">bool</span> <span class=\"fn-param\">$nullable</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Metadata</span> <span class=\"fn-param\">$metadata</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">XMLDefinition</span>
                </div>
                            `
            return div
        },
        apply: snippet("\\Flow\\ETL\\DSL\\xml_schema(" + "$" + "{" + "1:name" + "}" + ", " + "$" + "{" + "2:nullable" + "}" + ", " + "$" + "{" + "3:metadata" + "}" + ")"),
        boost: 10
    }    ]

/**
 * DSL function completion source for CodeMirror
 * @param {CompletionContext} context
 * @returns {CompletionResult|null}
 */
export function dslCompletions(context) {
    // Get text before cursor to check context
    const maxLookback = 100
    const docText = context.state.doc.toString()
    const startPos = Math.max(0, context.pos - maxLookback)
    const textBefore = docText.slice(startPos, context.pos)

    // Don't show DSL functions after -> (method chaining context)
    // DSL functions are standalone, not methods
    if (new RegExp('->\\w*$').test(textBefore)) {
        return null
    }

    // Match word being typed
    const word = context.matchBefore(/\w+/)

    // If no word and not explicit, don't show completions
    if (!word && !context.explicit) {
        return null
    }

    // Filter functions based on what's being typed
    const prefix = word ? word.text.toLowerCase() : ''
    const options = dslFunctions.filter(func =>
        !prefix || func.label.toLowerCase().startsWith(prefix)
    )

    return {
        from: word ? word.from : context.pos,
        options: options,
        validFor: new RegExp('^\\w*$')  // Reuse while typing word characters
    }
}
